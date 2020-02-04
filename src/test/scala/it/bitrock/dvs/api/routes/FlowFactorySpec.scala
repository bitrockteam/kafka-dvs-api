package it.bitrock.dvs.api.routes

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.{Sink, Source}
import it.bitrock.dvs.api.BaseSpec
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

import scala.concurrent.ExecutionContextExecutor

class FlowFactorySpec extends BaseSpec with ScalatestRouteTest with ScalaFutures with IntegrationPatience {

  implicit private val ec: ExecutionContextExecutor = system.dispatcher

  "FlowFactory" should {
    "create a flow linked to a processor that evaluate only valid messages" in {
      val flow = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val validMessage1 =
        """
          | {
          |   "@type": "startTotal"
          | }
          |""".stripMargin

      val validMessage2 =
        """
          | {
          |   "@type": "stopTotal"
          | }
          |""".stripMargin

      val invalidMessage = "invalid"

      val result = Source(List(TextMessage(validMessage1), TextMessage(invalidMessage), TextMessage(validMessage2)))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(2)
        .runWith(Sink.seq)
      whenReady(result) { messages =>
        messages shouldBe Vector(TextMessage("received StartTotals"), TextMessage("received StopTotals"))
      }
    }

    "parse CoordinatesBox messages" in {
      val flow = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val validMessage =
        """
          | {
          |   "@type": "startFlightList",
          |   "leftHighLat": 1.0,
          |   "leftHighLon": 1.0,
          |   "rightLowLat": 1.0,
          |   "rightLowLon": 1.0
          | }
          |""".stripMargin

      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(validMessage))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result) { messages =>
        messages shouldBe TextMessage("received CoordinatesBox(1.0,1.0,1.0,1.0)")
      }
    }

    "parse StopFlightList messages" in {
      val flow = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val validMessage =
        """
          | {
          |   "@type": "stopFlightList"
          | }
          |""".stripMargin

      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(validMessage))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result) { messages =>
        messages shouldBe TextMessage("received StopFlightList")
      }
    }

    "parse StartTotals messages" in {
      val flow = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val validMessage =
        """
          | {
          |   "@type": "startTotal"
          | }
          |""".stripMargin

      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(validMessage))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result) { messages =>
        messages shouldBe TextMessage("received StartTotals")
      }
    }

    "parse StopTotals messages" in {
      val flow = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val validMessage =
        """
          | {
          |   "@type": "stopTotal"
          | }
          |""".stripMargin

      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(validMessage))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result) { messages =>
        messages shouldBe TextMessage("received StopTotals")
      }
    }

    "parse StartTops messages" in {
      val flow = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val validMessage =
        """
          | {
          |   "@type": "startTop"
          | }
          |""".stripMargin

      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(validMessage))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result) { messages =>
        messages shouldBe TextMessage("received StartTops")
      }
    }

    "parse StopTops messages" in {
      val flow = FlowFactory.messageExchangeFlowFactory(dummyEchoProcessor).flow
      val validMessage =
        """
          | {
          |   "@type": "stopTop"
          | }
          |""".stripMargin

      val invalidMessage = "invalid"

      val result = Source
        .single(TextMessage(validMessage))
        .concat(Source.repeat(TextMessage(invalidMessage)))
        .via(flow)
        .take(1)
        .runWith(Sink.head)
      whenReady(result) { messages =>
        messages shouldBe TextMessage("received StopTops")
      }
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
