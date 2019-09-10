package it.bitrock.kafkaflightstream.api.routes

import akka.NotUsed
import akka.actor.PoisonPill
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.api.core.MessageProcessorFactory

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait FlowFactory {
  def flow: Flow[Message, Message, NotUsed]
}

class FlowFactoryImpl(processorFactory: MessageProcessorFactory)(
    implicit ec: ExecutionContext,
    materializer: ActorMaterializer
) extends FlowFactory
    with LazyLogging {

  final private val SourceActorBufferSize       = 1000
  final private val SourceActorOverflowStrategy = OverflowStrategy.dropHead

  def flow: Flow[Message, Message, NotUsed] = {
    val (sourceActorRef, publisher) = Source
      .actorRef[String](SourceActorBufferSize, SourceActorOverflowStrategy)
      .toMat(BroadcastHub.sink)(Keep.both)
      .run()

    val processor = processorFactory.build(sourceActorRef)

    logger.debug("Going to generate a flow")

    Flow
      .fromSinkAndSourceCoupled(Sink.ignore, publisher.map(TextMessage(_)))
      .watchTermination() { (termWatchBefore, termWatchAfter) =>
        termWatchAfter.onComplete {
          case Success(_) =>
            logger.info("Web-socket connection closed normally")

            processor ! PoisonPill
          case Failure(e) =>
            logger.warn("Web-socket connection terminated", e)

            processor ! PoisonPill
        }

        termWatchBefore
      }
  }

}
