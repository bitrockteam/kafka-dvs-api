package it.bitrock.kafkaflightstream.api

import akka.actor.{ActorSystem, Scheduler}
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
  lazy val scheduler: Scheduler                = system.scheduler

  val httpClientFactory                        = new HttpClientFactoryImpl
  val ksqlClientFactory: KsqlClientFactoryImpl = new KsqlClientFactoryImpl(httpClientFactory)
  val ksqlOps                                  = new KsqlOpsImpl(ksqlClientFactory)
  val internalsService                         = new InternalsService
  val ksqlService                              = new KsqlService(ksqlOps, config.server.websocket)
  val sessionService                           = new SessionService(ksqlOps)

  val flightKafkaConsumerWrapperFactory = flightKafkaConsumerFactory(config.kafka)
  val flightMessageProcessorFactory =
    new FlightMessageProcessorFactoryImpl(config.server.websocket, config.kafka, flightKafkaConsumerWrapperFactory)
  val flightFlowFactory = new FlowFactoryImpl(flightMessageProcessorFactory)

  val flightListKafkaConsumerWrapperFactory = flightListKafkaConsumerFactory(config.kafka)
  val flightListMessageProcessorFactory =
    new FlightListMessageProcessorFactoryImpl(config.server.websocket, config.kafka, flightListKafkaConsumerWrapperFactory)
  val flightListFlowFactory = new FlowFactoryImpl(flightListMessageProcessorFactory)

  val topsKafkaConsumerWrapperFactory = topsKafkaConsumerFactory(config.kafka)
  val topsMessageProcessorFactory =
    new TopsMessageProcessorFactoryImpl(config.server.websocket, config.kafka, topsKafkaConsumerWrapperFactory)
  val topsFlowFactory = new FlowFactoryImpl(topsMessageProcessorFactory)

  val totalsKafkaConsumerWrapperFactory = totalsKafkaConsumerFactory(config.kafka)
  val totalsMessageProcessorFactory =
    new TotalsMessageProcessorFactoryImpl(config.server.websocket, config.kafka, totalsKafkaConsumerWrapperFactory)
  val totalsFlowFactory = new FlowFactoryImpl(totalsMessageProcessorFactory)

  val ksqlKafkaConsumerWrapperFactory = ksqlKafkaConsumerFactory(config.kafka)
  val ksqlMessageProcessorFactory =
    new KsqlMessageProcessorFactoryImpl(config.server.websocket, config.kafka, ksqlKafkaConsumerWrapperFactory)
  val ksqlFlowFactory = new FlowFactoryImpl(
    ksqlMessageProcessorFactory,
    Option(
      streamId => scheduler.scheduleOnce(config.server.websocket.cleanupDelay)(ksqlOps.cleanSession(IndexedSeq(streamId)))
    )
  )

  val flowFactories: Map[FlowFactoryKey, FlowFactory] =
    Map(
      flightFlowFactoryKey     -> flightFlowFactory,
      flightListFlowFactoryKey -> flightListFlowFactory,
      topsFlowFactoryKey       -> topsFlowFactory,
      totalsFlowFactoryKey     -> totalsFlowFactory,
      ksqlFlowFactoryKey       -> ksqlFlowFactory
    )

  val api: Route                           = new Routes(flowFactories, config.server.websocket).routes
  val apiRest: Route                       = new RestRoutes(internalsService, ksqlService, sessionService).routes
  val bindingFuture: Future[ServerBinding] = Http().bindAndHandle(api ~ apiRest, host, port)

  bindingFuture.map { serverBinding =>
    logger.info(s"Exposing to ${serverBinding.localAddress}")
  }

  sys.addShutdownHook {
    logger.info("Shutting down")

    val resourcesClosed = for {
      binding <- bindingFuture
      _       <- binding.terminate(hardDeadline = 3.seconds)
      _       <- httpClientFactory.close()
      t       <- system.terminate()
    } yield t

    Await.result(resourcesClosed, 10.seconds)
  }
}
