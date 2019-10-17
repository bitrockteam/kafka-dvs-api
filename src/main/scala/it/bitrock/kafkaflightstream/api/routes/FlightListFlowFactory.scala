package it.bitrock.kafkaflightstream.api.routes

import akka.NotUsed
import akka.actor.{PoisonPill, Terminated}
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.api.core.MessageProcessorFactory
import it.bitrock.kafkaflightstream.api.definitions.{CoordinatesBox, JsonSupport}
import spray.json._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class FlightListFlowFactory(processorFactory: MessageProcessorFactory, cleanUp: Option[String => Any] = None)(
    implicit ec: ExecutionContext,
    materializer: ActorMaterializer
) extends FlowFactory
    with JsonSupport
    with LazyLogging {

  final private val SourceActorBufferSize       = 1000
  final private val SourceActorOverflowStrategy = OverflowStrategy.dropHead

  def flow(identifier: String): Flow[Message, Message, NotUsed] = {
    val (sourceActorRef, publisher) = Source
      .actorRef[String](SourceActorBufferSize, SourceActorOverflowStrategy)
      .toMat(BroadcastHub.sink)(Keep.both)
      .run()

    val processor = processorFactory.build(sourceActorRef, identifier)

    val sink =
      Flow
        .fromFunction(parseMessage)
        .collect {
          case Some(x) => x
        }
        .to(Sink.actorRef(processor, Terminated))

    logger.debug("Going to generate a flight list flow")

    Flow
      .fromSinkAndSourceCoupled(sink, publisher.map(TextMessage(_)))
      .watchTermination() { (termWatchBefore, termWatchAfter) =>
        termWatchAfter.onComplete {
          case Success(_) =>
            logger.info("Web-socket connection closed normally")
            cleanUp.foreach(_(identifier))
            processor ! PoisonPill
          case Failure(e) =>
            logger.warn("Web-socket connection terminated", e)
            cleanUp.foreach(_(identifier))
            processor ! PoisonPill
        }
        termWatchBefore
      }
  }

  private def parseMessage: Message => Option[CoordinatesBox] = {
    case TextMessage.Strict(txt) =>
      logger.debug(s"Got message: $txt")
      Try(txt.parseJson.convertTo[CoordinatesBox])
        .fold(e => {
          logger.warn(s"Failed to parse JSON message $txt", e)
          None
        }, Option(_))
    case m =>
      logger.trace(s"Got non-TextMessage, ignoring it: $m")
      None
  }

}
