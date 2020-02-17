package it.bitrock.dvs.api.routes

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.{Sink, Source}
import it.bitrock.dvs.api.BaseSpec
import it.bitrock.dvs.api.TestValues._
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

import scala.concurrent.ExecutionContextExecutor

class FlowFactorySpec extends BaseSpec with ScalatestRouteTest with ScalaFutures with IntegrationPatience {

  implicit private val ec: ExecutionContextExecutor = system.dispatcher

  "FlowFactory" should {
    "create a flow linked to a processor that evaluate only valid messages" in {
      val flow           = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val invalidMessage = "invalid"

      val result = Source(List(TextMessage(startTotal), TextMessage(invalidMessage), TextMessage(stopTotal)))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(2)
        .runWith(Sink.seq)
      whenReady(result) { messages =>
        messages shouldBe Vector(TextMessage("received StartTotals"), TextMessage("received StopTotals"))
      }
    }

    "parse CoordinatesBox messages" in {
      val flow           = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(coordinatesBox))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result)(messages => messages shouldBe TextMessage("received CoordinatesBox(23.6,67.9,37.98,43.45)"))
    }

    "parse StopFlightList messages" in {
      val flow           = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(stopFlightList))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result)(messages => messages shouldBe TextMessage("received StopFlightList"))
    }

    "parse StartTotals messages" in {
      val flow           = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(startTotal))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result)(messages => messages shouldBe TextMessage("received StartTotals"))
    }

    "parse StopTotals messages" in {
      val flow           = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(stopTotal))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result)(messages => messages shouldBe TextMessage("received StopTotals"))
    }

    "parse StartTops messages" in {
      val flow           = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(startTop))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result)(messages => messages shouldBe TextMessage("received StartTops"))
    }

    "parse StopTops messages" in {
      val flow           = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(stopTop))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result)(messages => messages shouldBe TextMessage("received StopTops"))
    }
  }

  private def dummyEchoProcessor: MessageDispatcherFactory = new MessageDispatcherFactory {
    override def build(sourceActorRef: ActorRef): ActorRef =
      system.actorOf(Props(new Actor {
        override def receive: Receive = {
          case m => sourceActorRef ! s"received $m"
        }
      }))
  }
}
