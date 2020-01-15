package it.bitrock.dvs.api.routes

import akka.NotUsed
import akka.actor.PoisonPill
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait FlowFactory {
  def flow(identifier: String): Flow[Message, Message, NotUsed]
}

class FlowFactoryImpl(processorFactory: MessageDispatcherFactory, cleanUp: Option[String => Any] = None)(
    implicit ec: ExecutionContext,
    materializer: ActorMaterializer
) extends FlowFactory
    with LazyLogging {

  final private val SourceActorBufferSize       = 1000
  final private val SourceActorOverflowStrategy = OverflowStrategy.dropHead

  def flow(identifier: String): Flow[Message, Message, NotUsed] = {
    val (sourceActorRef, publisher) = Source
      .actorRef[String](SourceActorBufferSize, SourceActorOverflowStrategy)
      .toMat(BroadcastHub.sink)(Keep.both)
      .run()

    val processor = processorFactory.build(sourceActorRef, identifier)

    logger.debug("Going to generate a flow")

    Flow
      .fromSinkAndSourceCoupled(Sink.ignore, publisher.map(TextMessage(_)))
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
}
