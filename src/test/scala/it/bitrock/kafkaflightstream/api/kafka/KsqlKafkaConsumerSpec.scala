package it.bitrock.kafkaflightstream.api.kafka

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import it.bitrock.kafkaflightstream.api.config.{AppConfig, KafkaConfig}
import it.bitrock.kafkaflightstream.api.definitions.KsqlStreamDataResponse
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper.NoMessage
import it.bitrock.kafkaflightstream.api.{BaseSpec, TestValues}
import it.bitrock.kafkageostream.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.kafkageostream.testcommons.FixtureLoanerAnyResult
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import scala.util.Random

class KsqlKafkaConsumerSpec
    extends TestKit(ActorSystem("KsqlKafkaConsumerSpec"))
    with EmbeddedKafka
    with BaseSpec
    with TestValues
    with Eventually
    with BeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(7.seconds)

  private val topic: String = "test_topic_" + Random.nextLong

  "Kafka Consumer" should {

    "forward any record it reads from its subscribed topics to the configured processor, in order" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, _, valueSerde, _, processorProbe, pollMessages) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val stringSerde: Serde[String]          = valueSerde

        val message         = DefaultKsqlMessage.toString
        val expectedMessage = KsqlStreamDataResponse(data = message)

        withRunningKafka {

          // Using eventually to ignore any warm up time Kafka could have
          eventually {
            publishToKafka(topic, message)
            pollMessages()

            processorProbe.expectMsg(expectedMessage)
          }
        }
    }

    "ignore messages present on a topic before Consumer is started" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, _, valueSerde, keySerde, processorProbe, pollMessages) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val stringSerde: Serde[String]          = valueSerde
        implicit val longSerde: Serde[Long]              = keySerde

        val message = DefaultKsqlMessage.toString

        withRunningKafka {
          // Publish to a topic before consumer is started
          publishToKafka(topic, message)

          val deadline = patienceConfig.interval.fromNow

          while (deadline.hasTimeLeft) {
            pollMessages()
            processorProbe.expectMsg(NoMessage)

          }

          val (_, response) = consumeFirstKeyedMessageFrom[Long, String](topic)

          response shouldBe message
        }
    }
  }

  object ResourceLoaner extends FixtureLoanerAnyResult[Resource] {
    override def withFixture(body: Resource => Any): Any = {
      implicit val embKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig()
      val config                                       = AppConfig.load
      val kafkaConfig: KafkaConfig = config.kafka.copy(
        bootstrapServers = s"localhost:${embKafkaConfig.kafkaPort}",
        schemaRegistryUrl = URI.create(s"http://localhost:${embKafkaConfig.schemaRegistryPort}"),
        consumer = config.kafka.consumer.copy(startupRewind = Duration.Zero)
      )
      val valueSerde           = Serdes.String
      val keySerde             = Serdes.Long.asInstanceOf[Serde[Long]]
      val processor            = TestProbe()
      val kafkaConsumerWrapper = KafkaConsumerWrapperFactory.ksqlKafkaConsumerFactory(kafkaConfig).build(processor.ref, List(topic))

      try {
        body(
          Resource(
            embKafkaConfig,
            kafkaConfig,
            valueSerde,
            keySerde,
            processor,
            () => kafkaConsumerWrapper.pollMessages()
          )
        )
      } finally {
        kafkaConsumerWrapper.close()
      }
    }
  }

  override def afterAll: Unit = {
    shutdown()
    super.afterAll()
  }

  final case class Resource(
      embeddedKafkaConfig: EmbeddedKafkaConfig,
      kafkaConfig: KafkaConfig,
      valueSerde: Serde[String],
      keySerde: Serde[Long],
      processorProbe: TestProbe,
      pollMessages: () => Unit
  )
}
