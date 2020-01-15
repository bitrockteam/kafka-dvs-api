package it.bitrock.kafkaflightstream.api

import akka.actor.{ActorSystem, Scheduler}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.api.config.AppConfig
import it.bitrock.kafkaflightstream.api.core.factory.{
  FlightListMessageDispatcherFactoryImpl,
  TopsMessageDispatcherFactoryImpl,
  TotalsMessageDispatcherFactoryImpl
}
import it.bitrock.kafkaflightstream.api.core.poller._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapperFactory._
import it.bitrock.kafkaflightstream.api.routes._
import it.bitrock.kafkaflightstream.tags.FlowFactoryKey
import it.bitrock.kafkaflightstream.tags.TaggedTypes._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object Main extends App with LazyLogging {
  logger.info("Starting up")

  val config = AppConfig.load
  logger.debug(s"Loaded configuration: $config")

  val host: String = config.server.host
  val port: Int    = config.server.port

  implicit val system: ActorSystem             = ActorSystem("KafkaFlightStreamApi")
  implicit val ec: ExecutionContextExecutor    = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  lazy val scheduler: Scheduler                = system.scheduler

  val flightListKafkaConsumerWrapperFactory = flightListKafkaConsumerFactory(config.kafka)
  val flightListKafkaMessagePollerCache     = FlightListKafkaPollerCache.build(config.kafka, flightListKafkaConsumerWrapperFactory)
  val flightListMessageDispatcherFactory =
    new FlightListMessageDispatcherFactoryImpl(config.server.websocket, flightListKafkaMessagePollerCache)
  val flightListFlowFactory = new FlightListFlowFactory(flightListMessageDispatcherFactory)

  val topsKafkaConsumerWrapperFactory = topsKafkaConsumerFactory(config.kafka)
  val topsKafkaPollerCache            = TopsKafkaPollerCache.build(config.kafka, topsKafkaConsumerWrapperFactory)
  val topsMessageDispatcherFactory    = new TopsMessageDispatcherFactoryImpl(config.server.websocket, topsKafkaPollerCache)
  val topsFlowFactory                 = new FlowFactoryImpl(topsMessageDispatcherFactory)

  val totalsKafkaConsumerWrapperFactory = totalsKafkaConsumerFactory(config.kafka)
  val totalsKafkaPollerCache            = TotalsKafkaPollerCache.build(config.kafka, totalsKafkaConsumerWrapperFactory)
  val totalsMessageDispatcherFactory    = new TotalsMessageDispatcherFactoryImpl(config.server.websocket, totalsKafkaPollerCache)
  val totalsFlowFactory                 = new FlowFactoryImpl(totalsMessageDispatcherFactory)

  val flowFactories: Map[FlowFactoryKey, FlowFactory] =
    Map(
      flightListFlowFactoryKey -> flightListFlowFactory,
      topsFlowFactoryKey       -> topsFlowFactory,
      totalsFlowFactoryKey     -> totalsFlowFactory
    )

  val api: Route                           = new Routes(flowFactories, config.server.websocket).routes
  val bindingFuture: Future[ServerBinding] = Http().bindAndHandle(api, host, port)

  bindingFuture.map { serverBinding =>
    logger.info(s"Exposing to ${serverBinding.localAddress}")
  }

  sys.addShutdownHook {
    logger.info("Shutting down")

    val resourcesClosed = for {
      binding <- bindingFuture
      _       <- binding.terminate(hardDeadline = 3.seconds)
      t       <- system.terminate()
    } yield t

    Await.result(resourcesClosed, 10.seconds)
  }
}
