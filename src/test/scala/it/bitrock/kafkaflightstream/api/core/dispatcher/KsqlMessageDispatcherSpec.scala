package it.bitrock.kafkaflightstream.api.core.dispatcher

import java.net.URI

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import it.bitrock.kafkaflightstream.api.config.{ConsumerConfig, KafkaConfig, KsqlConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.core.KsqlMessageDispatcherFactoryImpl
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import it.bitrock.kafkaflightstream.api.{BaseSpec, TestValues}
import it.bitrock.kafkageostream.testcommons.FixtureLoanerAnyResult
import org.scalatest.BeforeAndAfterAll
import spray.json._

import scala.concurrent.duration._
import scala.util.Random

class KsqlMessageDispatcherSpec
    extends TestKit(ActorSystem("KsqlMessageProcessorSpec"))
    with BaseSpec
    with ImplicitSender
    with BeforeAndAfterAll
    with JsonSupport
    with TestValues {

  import KsqlMessageDispatcherSpec._

  private val topic: String = "test_topic_" + Random.nextLong

  "Ksql Message Dispatcher" should {
    "trigger Kafka Consumer polling" when {

      "it starts" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, pollProbe, sourceProbe) =>
          new KsqlMessageDispatcherFactoryImpl(websocketConfig, kafkaConfig, consumerFactory).build(sourceProbe.ref, topic)

          pollProbe.expectMsg(PollingTriggered)
      }

      "a KsqlReceived message is received, but only after a delay" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, pollProbe, sourceProbe) =>
          val messageDispatcher =
            new KsqlMessageDispatcherFactoryImpl(websocketConfig, kafkaConfig, consumerFactory).build(sourceProbe.ref, topic)

          // First message is sent when processor starts up
          pollProbe.expectMsg(PollingTriggered)

          messageDispatcher ! KsqlStreamDataResponse(DefaultKsqlMessage.toString)

          pollProbe.expectNoMessage(websocketConfig.throttleDuration)
          pollProbe.expectMsg(PollingTriggered)
      }

    }

    "forward a JSON to source actor" when {

      "a KsqlReceived message is received" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, _, sourceProbe) =>
          val messageDispatcher =
            new KsqlMessageDispatcherFactoryImpl(websocketConfig, kafkaConfig, consumerFactory).build(sourceProbe.ref, topic)
          val msg = KsqlStreamDataResponse(DefaultKsqlMessage.toString)

          messageDispatcher ! msg

          val expectedResult = msg.toJson.toString

          sourceProbe.expectMsg(expectedResult)
      }

    }

  }

  object ResourceLoaner extends FixtureLoanerAnyResult[Resource] {
    override def withFixture(body: Resource => Any): Any = {
      val websocketConfig = WebsocketConfig(1.second, 0.second, "not-used", "not-used", "not-used", "not-used", "not-used", "not-used")
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
          ConsumerConfig(1.second, Duration.Zero),
          KsqlConfig(java.net.URI.create("http://www.example.com"), "")
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

object KsqlMessageDispatcherSpec {

  final case class Resource(
      websocketConfig: WebsocketConfig,
      kafkaConfig: KafkaConfig,
      consumerFactory: KafkaConsumerWrapperFactory,
      pollProbe: TestProbe,
      sourceProbe: TestProbe
  )

  case object PollingTriggered

  class TestKafkaConsumerWrapperFactory(pollActorRef: ActorRef) extends KafkaConsumerWrapperFactory {

    override def build(processor: ActorRef, topics: Seq[String]): KafkaConsumerWrapper = new KafkaConsumerWrapper {

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
