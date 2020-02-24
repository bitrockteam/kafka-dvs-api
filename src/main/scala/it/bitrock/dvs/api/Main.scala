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
import it.bitrock.dvs.api.model.KafkaPollerHub
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

  private val topsKafkaConsumerWrapperFactory = topsKafkaConsumerFactory(config.kafka)
  private val topsKafkaPollerCache            = TopsKafkaPollerCache.build(config.kafka, topsKafkaConsumerWrapperFactory)

  private val totalsKafkaConsumerWrapperFactory = totalsKafkaConsumerFactory(config.kafka)
  private val totalsKafkaPollerCache            = TotalsKafkaPollerCache.build(config.kafka, totalsKafkaConsumerWrapperFactory)

  private val kafkaPollerHub = KafkaPollerHub(flightListKafkaMessagePollerCache, topsKafkaPollerCache, totalsKafkaPollerCache)

  private val globalMessageDispatcherFactory =
    MessageDispatcherFactory.globalMessageDispatcherFactory(kafkaPollerHub, config.server.webSocket)
  private val globalFlowFactory = FlowFactory.messageExchangeFlowFactory(globalMessageDispatcherFactory)

  private val restRoutes                           = Routes.healthRoutes(config.server.rest)
  private val webSocketRoutes                      = Routes.webSocketRoutes(config.server.webSocket, globalFlowFactory)
  private val api: Route                           = webSocketRoutes ~ restRoutes
  private val bindingFuture: Future[ServerBinding] = Http().bindAndHandle(api, host, port)

  bindingFuture.map(serverBinding => logger.info(s"Exposing to ${serverBinding.localAddress}"))

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
