package it.bitrock.kafkaflightstream.api

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteConcatenation._
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.api.config.AppConfig
import it.bitrock.kafkaflightstream.api.core._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapperFactory._
import it.bitrock.kafkaflightstream.api.routes._
import it.bitrock.kafkaflightstream.api.services._
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

  val internalsService = new InternalsService

  val flightKafkaConsumerWrapperFactory = flightKafkaConsumerFactory(config.kafka)
  val flightMessageProcessorFactory =
    new FlightMessageProcessorFactoryImpl(config.server.websocket, config.kafka, flightKafkaConsumerWrapperFactory)
  val flightFlowFactory = new FlowFactoryImpl(flightMessageProcessorFactory)

  val topsKafkaConsumerWrapperFactory = topsKafkaConsumerFactory(config.kafka)
  val topsMessageProcessorFactory =
    new TopsMessageProcessorFactoryImpl(config.server.websocket, config.kafka, topsKafkaConsumerWrapperFactory)
  val topsFlowFactory = new FlowFactoryImpl(topsMessageProcessorFactory)

  val totalsKafkaConsumerWrapperFactory = totalsKafkaConsumerFactory(config.kafka)
  val totalsMessageProcessorFactory =
    new TotalsMessageProcessorFactoryImpl(config.server.websocket, config.kafka, totalsKafkaConsumerWrapperFactory)
  val totalsFlowFactory = new FlowFactoryImpl(totalsMessageProcessorFactory)

  val flowFactories: Map[FlowFactoryKey, FlowFactory] =
    Map(
      flightFlowFactoryKey -> flightFlowFactory,
      topsFlowFactoryKey   -> topsFlowFactory,
      totalsFlowFactoryKey -> totalsFlowFactory
    )

  val api: Route                           = new Routes(flowFactories, config.server.websocket).routes
  val apiRest: Route                       = new RestRoutes(internalsService).routes
  val bindingFuture: Future[ServerBinding] = Http().bindAndHandle(api ~ apiRest, host, port)

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
