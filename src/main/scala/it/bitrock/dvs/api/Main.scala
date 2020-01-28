package it.bitrock.dvs.api

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteConcatenation._
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.dvs.api.config.AppConfig
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import it.bitrock.dvs.api.core.poller._
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapperFactory._
import it.bitrock.dvs.api.routes.HealthRoute._
import it.bitrock.dvs.api.routes.Routes.FlowFactories
import it.bitrock.dvs.api.routes._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object Main extends App with LazyLogging {
  logger.info("Starting up")

  private val config = AppConfig.load
  logger.debug(s"Loaded configuration: $config")

  private val host: String = config.server.host
  private val port: Int    = config.server.port

  implicit private val system: ActorSystem          = ActorSystem("KafkaDVSApi")
  implicit private val ec: ExecutionContextExecutor = system.dispatcher

  private val flightListKafkaConsumerWrapperFactory = flightListKafkaConsumerFactory(config.kafka)
  private val flightListKafkaMessagePollerCache =
    FlightListKafkaPollerCache.build(config.kafka, flightListKafkaConsumerWrapperFactory)
  private val flightListMessageDispatcherFactory =
    MessageDispatcherFactory.flightListMessageDispatcherFactory(
      flightListKafkaMessagePollerCache,
      config.server.webSocket
    )
  private val flightListFlowFactory = FlowFactory.messageExchangeFlowFactory(flightListMessageDispatcherFactory)

  private val topsKafkaConsumerWrapperFactory = topsKafkaConsumerFactory(config.kafka)
  private val topsKafkaPollerCache            = TopsKafkaPollerCache.build(config.kafka, topsKafkaConsumerWrapperFactory)
  private val topsMessageDispatcherFactory =
    MessageDispatcherFactory.topsMessageDispatcherFactory(topsKafkaPollerCache, config.server.webSocket)
  private val topsFlowFactory = FlowFactory.flightFlowFactory(topsMessageDispatcherFactory)

  private val totalsKafkaConsumerWrapperFactory = totalsKafkaConsumerFactory(config.kafka)
  private val totalsKafkaPollerCache            = TotalsKafkaPollerCache.build(config.kafka, totalsKafkaConsumerWrapperFactory)
  private val totalsMessageDispatcherFactory =
    MessageDispatcherFactory.totalsMessageDispatcherFactory(totalsKafkaPollerCache, config.server.webSocket)
  private val totalsFlowFactory = FlowFactory.flightFlowFactory(totalsMessageDispatcherFactory)

  private val flowFactories =
    FlowFactories(
      flightListFlowFactory = flightListFlowFactory,
      topsFlowFactory = topsFlowFactory,
      totalsFlowFactory = totalsFlowFactory
    )

  private val webSocketRoutes                      = Routes.webSocketRoutes(config.server.webSocket, flowFactories)
  private val api: Route                           = webSocketRoutes ~ healthCheckRoute
  private val bindingFuture: Future[ServerBinding] = Http().bindAndHandle(api, host, port)

  bindingFuture.map { serverBinding =>
    logger.info(s"Exposing to ${serverBinding.localAddress}")
  }

  sys.addShutdownHook {
    logger.info("Shutting down")

    val resourcesClosed = for {
      binding <- bindingFuture
      _       <- binding.terminate(hardDeadline = 3.seconds)
      _       <- system.terminate()
    } yield ()

    Await.result(resourcesClosed, 10.seconds)
  }
}
