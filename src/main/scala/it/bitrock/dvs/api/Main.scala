package it.bitrock.dvs.api

import akka.actor.{ActorSystem, Scheduler}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.dvs.api.Tags.FlowFactoryKey
import it.bitrock.dvs.api.Tags.TaggedTypes._
import it.bitrock.dvs.api.config.AppConfig
import it.bitrock.dvs.api.core.factory.{
  FlightListMessageDispatcherFactoryImpl,
  TopsMessageDispatcherFactoryImpl,
  TotalsMessageDispatcherFactoryImpl
}
import it.bitrock.dvs.api.core.poller._
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapperFactory._
import it.bitrock.dvs.api.routes._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object Main extends App with LazyLogging {
  logger.info("Starting up")

  val config = AppConfig.load
  logger.debug(s"Loaded configuration: $config")

  val host: String = config.server.host
  val port: Int    = config.server.port

  implicit val system: ActorSystem             = ActorSystem("KafkaDVSApi")
  implicit val ec: ExecutionContextExecutor    = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  lazy val scheduler: Scheduler                = system.scheduler

  val flightListKafkaConsumerWrapperFactory = flightListKafkaConsumerFactory(config.kafka)
  val flightListKafkaMessagePollerCache     = FlightListKafkaPollerCache.build(config.kafka, flightListKafkaConsumerWrapperFactory)
  val flightListMessageDispatcherFactory =
    new FlightListMessageDispatcherFactoryImpl(config.server.websocket, flightListKafkaMessagePollerCache)
  val flightListFlowFactory = FlowFactory.flightListFlowFactory(flightListMessageDispatcherFactory)

  val topsKafkaConsumerWrapperFactory = topsKafkaConsumerFactory(config.kafka)
  val topsKafkaPollerCache            = TopsKafkaPollerCache.build(config.kafka, topsKafkaConsumerWrapperFactory)
  val topsMessageDispatcherFactory    = new TopsMessageDispatcherFactoryImpl(config.server.websocket, topsKafkaPollerCache)
  val topsFlowFactory                 = FlowFactory.flightFlowFactory(topsMessageDispatcherFactory)

  val totalsKafkaConsumerWrapperFactory = totalsKafkaConsumerFactory(config.kafka)
  val totalsKafkaPollerCache            = TotalsKafkaPollerCache.build(config.kafka, totalsKafkaConsumerWrapperFactory)
  val totalsMessageDispatcherFactory    = new TotalsMessageDispatcherFactoryImpl(config.server.websocket, totalsKafkaPollerCache)
  val totalsFlowFactory                 = FlowFactory.flightFlowFactory(totalsMessageDispatcherFactory)

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
      _       <- system.terminate()
    } yield ()

    Await.result(resourcesClosed, 10.seconds)
  }
}
