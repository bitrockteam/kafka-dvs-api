package it.bitrock.dvs.api

import java.net.URI

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import it.bitrock.dvs.api.BaseTestKit._
import it.bitrock.dvs.api.config.{ConsumerConfig, KafkaConfig, WebSocketConfig}
import it.bitrock.dvs.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import it.bitrock.testcommons.{AsyncSuite, FixtureLoanerAnyResult, Suite}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.{AnyWordSpecLike, AsyncWordSpecLike}

import scala.concurrent.duration._
import scala.util.Random

/**
  * Base trait to test classes.
  */
trait BaseSpec extends Suite with AnyWordSpecLike

/**
  * Base trait to test asynchronous assertions.
  */
trait BaseAsyncSpec extends AsyncSuite with AsyncWordSpecLike

abstract class BaseTestKit
    extends TestKit(ActorSystem("BaseSpec"))
    with BaseSpec
    with ImplicitSender
    with BeforeAndAfterAll
    with JsonSupport
    with TestValues {

  object ResourceLoanerPoller extends FixtureLoanerAnyResult[ResourcePoller] {
    override def withFixture(body: ResourcePoller => Any): Any = {
      val pollProbe       = TestProbe(s"poll-probe-${Random.nextInt()}")
      val consumerFactory = new TestKafkaConsumerWrapperFactory(pollProbe.ref)
      body(
        ResourcePoller(
          kafkaConfig,
          consumerFactory,
          pollProbe
        )
      )
    }
  }

  object ResourceLoanerDispatcher extends FixtureLoanerAnyResult[ResourceDispatcher] {
    override def withFixture(body: ResourceDispatcher => Any): Any = {
      val pollProbe       = TestProbe(s"poll-probe-${Random.nextInt()}")
      val sourceProbe     = TestProbe(s"source-probe-${Random.nextInt()}")
      val consumerFactory = new TestKafkaConsumerWrapperFactory(pollProbe.ref)
      body(
        ResourceDispatcher(
          webSocketConfig,
          kafkaConfig,
          consumerFactory,
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

object BaseTestKit {
  val kafkaConfig: KafkaConfig =
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
      ConsumerConfig(1.second, Duration.Zero)
    )

  val webSocketConfig: WebSocketConfig =
    WebSocketConfig(
      1000,
      0.second,
      "not-used",
      "not-used",
      "not-used",
      "not-used"
    )

  case object PollingTriggered

  final case class ResourcePoller(
      kafkaConfig: KafkaConfig,
      consumerFactory: KafkaConsumerWrapperFactory,
      pollProbe: TestProbe
  )

  final case class ResourceDispatcher(
      webSocketConfig: WebSocketConfig,
      kafkaConfig: KafkaConfig,
      consumerFactory: KafkaConsumerWrapperFactory,
      sourceProbe: TestProbe
  )

  class TestKafkaConsumerWrapperFactory(pollActorRef: ActorRef) extends KafkaConsumerWrapperFactory {
    override def build(processor: ActorRef, topics: Seq[String] = List()): KafkaConsumerWrapper = new KafkaConsumerWrapper {
      override def pollMessages(): Unit      = pollActorRef ! PollingTriggered
      override def close(): Unit             = ()
      override val maxPollRecords: Int       = 1
      override def moveTo(epoch: Long): Unit = ()
      override def pause(): Unit             = ()
      override def resume(): Unit            = ()
    }
  }
}
