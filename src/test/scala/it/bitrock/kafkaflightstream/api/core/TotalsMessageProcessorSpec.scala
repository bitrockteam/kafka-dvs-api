package it.bitrock.kafkaflightstream.api.core

import java.net.URI

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import it.bitrock.kafkaflightstream.api.config.{ConsumerConfig, KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import it.bitrock.kafkaflightstream.api.{BaseSpec, TestValues}
import it.bitrock.kafkageostream.testcommons.FixtureLoanerAnyResult
import org.scalatest.BeforeAndAfterAll
import spray.json._

import scala.concurrent.duration._
import scala.util.Random

class TotalsMessageProcessorSpec
    extends TestKit(ActorSystem("TotalsMessageProcessorSpec"))
    with BaseSpec
    with ImplicitSender
    with BeforeAndAfterAll
    with JsonSupport
    with TestValues {
  import TotalsMessageProcessorSpec._

  "Totals Message Processor" should {

    "trigger Kafka Consumer polling" when {
      "it starts" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, pollProbe, sourceProbe) =>
          new TotalsMessageProcessorFactoryImpl(websocketConfig, kafkaConfig, consumerFactory).build(sourceProbe.ref)
          pollProbe.expectMsg(PollingTriggered)
      }
      "a CountFlightStatus is received, but only after a delay" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, pollProbe, sourceProbe) =>
          val messageProcessor =
            new TotalsMessageProcessorFactoryImpl(websocketConfig, kafkaConfig, consumerFactory).build(sourceProbe.ref)
          // First message is sent when processor starts up
          pollProbe.expectMsg(PollingTriggered)
          messageProcessor ! CountFlight(DefaultStartTimeWindow, DefaultCountFlightAmount)
          pollProbe.expectNoMessage(websocketConfig.throttleDuration)
          pollProbe.expectMsg(PollingTriggered)
      }
      "a CountAirline is received, but only after a delay" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, pollProbe, sourceProbe) =>
          val messageProcessor =
            new TotalsMessageProcessorFactoryImpl(websocketConfig, kafkaConfig, consumerFactory).build(sourceProbe.ref)
          // First message is sent when processor starts up
          pollProbe.expectMsg(PollingTriggered)
          messageProcessor ! CountAirline(DefaultStartTimeWindow, DefaultCountAirlineAmount)
          pollProbe.expectNoMessage(websocketConfig.throttleDuration)
          pollProbe.expectMsg(PollingTriggered)
      }
    }

    "forward a JSON-formatted ApiEvent to source actor" when {
      "a CountFlightStatus is received" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, _, sourceProbe) =>
          val messageProcessor =
            new TotalsMessageProcessorFactoryImpl(websocketConfig, kafkaConfig, consumerFactory).build(sourceProbe.ref)
          val msg = CountFlight(DefaultStartTimeWindow, DefaultCountFlightAmount)
          messageProcessor ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
      "a CountAirline is received" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, _, sourceProbe) =>
          val messageProcessor =
            new TotalsMessageProcessorFactoryImpl(websocketConfig, kafkaConfig, consumerFactory).build(sourceProbe.ref)
          val msg = CountAirline(DefaultStartTimeWindow, DefaultCountAirlineAmount)
          messageProcessor ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
    }

  }

  object ResourceLoaner extends FixtureLoanerAnyResult[Resource] {
    override def withFixture(body: Resource => Any): Any = {
      val websocketConfig = WebsocketConfig(1.second, 0.second, "not-used", "not-used", "not-used", "not-used", "not-used")
      val kafkaConfig =
        KafkaConfig(
          "",
          URI.create("http://localhost:8080"),
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          ConsumerConfig(1.second, Duration.Zero)
        )
      val pollProbe                                    = TestProbe(s"poll-probe-${Random.nextInt()}")
      val sourceProbe                                  = TestProbe(s"source-probe-${Random.nextInt()}")
      val consumerFactory: KafkaConsumerWrapperFactory = new TestKafkaConsumerWrapperFactory(pollProbe.ref)

      body(
        Resource(
          websocketConfig,
          kafkaConfig,
          consumerFactory,
          pollProbe,
          sourceProbe
        )
      )
    }
  }

  override def afterAll: Unit = {
    shutdown()
    super.afterAll()
  }

}

object TotalsMessageProcessorSpec {

  final case class Resource(
      websocketConfig: WebsocketConfig,
      kafkaConfig: KafkaConfig,
      consumerFactory: KafkaConsumerWrapperFactory,
      pollProbe: TestProbe,
      sourceProbe: TestProbe
  )

  case object PollingTriggered

  class TestKafkaConsumerWrapperFactory(pollActorRef: ActorRef) extends KafkaConsumerWrapperFactory {

    override def build(processor: ActorRef, topics: Seq[String] = List()): KafkaConsumerWrapper = new KafkaConsumerWrapper {

      override def pollMessages(): Unit =
        pollActorRef ! PollingTriggered

      override def close(): Unit = ()

      override val maxPollRecords: Int = 1

      override def moveTo(epoch: Long): Unit = ()

      override def pause(): Unit = ()

      override def resume(): Unit = ()
    }

  }

}
