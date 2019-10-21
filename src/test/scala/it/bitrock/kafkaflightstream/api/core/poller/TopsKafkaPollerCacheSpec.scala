package it.bitrock.kafkaflightstream.api.core.poller

import java.net.URI

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import it.bitrock.kafkaflightstream.api.config.{ConsumerConfig, KafkaConfig, KsqlConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.core.TopsMessageDispatcherFactoryImpl
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import it.bitrock.kafkaflightstream.api.{BaseSpec, TestValues}
import it.bitrock.kafkageostream.testcommons.FixtureLoanerAnyResult
import org.scalatest.BeforeAndAfterAll
import spray.json._

import scala.concurrent.duration._
import scala.util.Random

class TopsKafkaPollerCacheSpec
    extends TestKit(ActorSystem("TopsKafkaPollerCacheSpec"))
    with BaseSpec
    with ImplicitSender
    with BeforeAndAfterAll
    with JsonSupport
    with TestValues {

  import TopsKafkaPollerCacheSpec._

  "Tops Message Dispatcher" should {

    "trigger Kafka Consumer polling" when {
      "it starts" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, pollProbe, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          new TopsMessageDispatcherFactoryImpl(websocketConfig, topsKafkaPollerCache).build(sourceProbe.ref)

          pollProbe expectMsg PollingTriggered
      }
      "a TopArrivalAirportList is received, but only after a delay" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, pollProbe, _) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topArrivalMessage    = TopArrivalAirportList(Seq(Airport("airport", 100)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topArrivalMessage
          pollProbe expectNoMessage websocketConfig.throttleDuration
          pollProbe expectMsg PollingTriggered
      }
      "a TopDepartureAirportList is received, but only after a delay" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, pollProbe, _) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topDepartureMessage  = TopDepartureAirportList(Seq(Airport("airport", 100)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topDepartureMessage
          pollProbe expectNoMessage websocketConfig.throttleDuration
          pollProbe expectMsg PollingTriggered
      }
      "a TopSpeedList is received, but only after a delay" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, pollProbe, _) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topSpeedMessage      = TopSpeedList(Seq(SpeedFlight("code", 1000.00)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topSpeedMessage
          pollProbe expectNoMessage websocketConfig.throttleDuration
          pollProbe expectMsg PollingTriggered
      }
      "a TopAirlineList is received, but only after a delay" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, pollProbe, _) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topAirlineMessage    = TopAirlineList(Seq(Airline("airline", 200)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topAirlineMessage
          pollProbe expectNoMessage websocketConfig.throttleDuration
          pollProbe expectMsg PollingTriggered
      }
    }

    "forward a JSON-formatted ApiEvent to source actor" when {
      "a TopArrivalAirportList is received" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, _, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher    = new TopsMessageDispatcherFactoryImpl(websocketConfig, topsKafkaPollerCache).build(sourceProbe.ref)
          val msg                  = TopArrivalAirportList()

          messageDispatcher ! msg

          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString

          sourceProbe.expectMsg(expectedResult)
      }
      "a TopDepartureAirportList is received" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, _, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher    = new TopsMessageDispatcherFactoryImpl(websocketConfig, topsKafkaPollerCache).build(sourceProbe.ref)
          val msg                  = TopDepartureAirportList()

          messageDispatcher ! msg

          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString

          sourceProbe.expectMsg(expectedResult)
      }
      "a TopSpeedList is received" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, _, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher    = new TopsMessageDispatcherFactoryImpl(websocketConfig, topsKafkaPollerCache).build(sourceProbe.ref)
          val msg                  = TopSpeedList()

          messageDispatcher ! msg

          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString

          sourceProbe.expectMsg(expectedResult)
      }
      "a TopAirlineList is received" in ResourceLoaner.withFixture {
        case Resource(websocketConfig, kafkaConfig, consumerFactory, _, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher    = new TopsMessageDispatcherFactoryImpl(websocketConfig, topsKafkaPollerCache).build(sourceProbe.ref)
          val msg                  = TopAirlineList()

          messageDispatcher ! msg

          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString

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

object TopsKafkaPollerCacheSpec {

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
