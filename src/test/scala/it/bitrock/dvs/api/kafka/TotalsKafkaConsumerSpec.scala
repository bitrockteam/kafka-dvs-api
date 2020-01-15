package it.bitrock.dvs.api.kafka

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import it.bitrock.dvs.api.config.{AppConfig, KafkaConfig}
import it.bitrock.dvs.api.definitions._
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper.NoMessage
import it.bitrock.dvs.api.{BaseSpec, TestValues}
import it.bitrock.dvs.model.avro.{CountAirline => KCountAirline, CountFlight => KCountFlight}
import it.bitrock.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.testcommons.FixtureLoanerAnyResult
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafka, EmbeddedKafkaConfig, _}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._

class TotalsKafkaConsumerSpec
    extends TestKit(ActorSystem("TotalsKafkaConsumerSpec"))
    with EmbeddedKafka
    with BaseSpec
    with TestValues
    with Eventually
    with BeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(6.seconds)

  "Kafka Consumer" should {

    "forward any record it reads from its subscribed topics to the configured processor, in order" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, kafkaConfig, _, processorProbe, pollMessages) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        withRunningKafka {
          eventually {
            publishToKafka(kafkaConfig.totalFlightTopic, KCountFlight(DefaultStartTimeWindow, DefaultCountFlightAmount))
            pollMessages()
            processorProbe.expectMsg(CountFlight(DefaultStartTimeWindow, DefaultCountFlightAmount))
          }
          eventually {
            publishToKafka(kafkaConfig.totalAirlineTopic, KCountAirline(DefaultStartTimeWindow, DefaultCountAirlineAmount))
            pollMessages()
            processorProbe.expectMsg(CountAirline(DefaultStartTimeWindow, DefaultCountAirlineAmount))
          }
        }
    }

    "ignore messages present on a topic before Consumer is started" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, kafkaConfig, totalKeySerde, processorProbe, pollMessages) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val stringSerde: Serde[String]          = totalKeySerde
        withRunningKafka {
          val kCountFlightStatus = KCountFlight(DefaultStartTimeWindow, DefaultCountFlightAmount)
          val kCountAirline      = KCountAirline(DefaultStartTimeWindow, DefaultCountAirlineAmount)
          // Publish to a topic before consumer is started
          publishToKafka(kafkaConfig.totalFlightTopic, kCountFlightStatus)
          publishToKafka(kafkaConfig.totalAirlineTopic, kCountAirline)

          val deadline = patienceConfig.interval.fromNow
          while (deadline.hasTimeLeft) {
            pollMessages()
            processorProbe.expectMsg(NoMessage)
          }

          val (_, expectedValue) = consumeFirstKeyedMessageFrom[String, KCountFlight](kafkaConfig.totalFlightTopic)
          expectedValue shouldBe kCountFlightStatus

          val (_, expectedAirlineCountValue) = consumeFirstKeyedMessageFrom[String, KCountAirline](kafkaConfig.totalAirlineTopic)
          expectedAirlineCountValue shouldBe kCountAirline
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
      val totalKeySerde: Serde[String] = Serdes.String
      val processor                    = TestProbe()
      val kafkaConsumerWrapper = KafkaConsumerWrapperFactory
        .totalsKafkaConsumerFactory(kafkaConfig)
        .build(processor.ref, List(kafkaConfig.totalFlightTopic, kafkaConfig.totalAirlineTopic))

      try {
        body(
          Resource(
            embKafkaConfig,
            kafkaConfig,
            totalKeySerde,
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
      totalKeySerde: Serde[String],
      processorProbe: TestProbe,
      pollMessages: () => Unit
  )
}