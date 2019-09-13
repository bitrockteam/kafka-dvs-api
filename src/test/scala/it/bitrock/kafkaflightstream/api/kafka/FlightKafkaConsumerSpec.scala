package it.bitrock.kafkaflightstream.api.kafka

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import it.bitrock.kafkaflightstream.api.config.{AppConfig, KafkaConfig}
import it.bitrock.kafkaflightstream.api.definitions.{AirlineInfo, AirplaneInfo, AirportInfo, FlightReceived, GeographyInfo}
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper.NoMessage
import it.bitrock.kafkaflightstream.api.{BaseSpec, TestValues}
import it.bitrock.kafkageostream.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.kafkaflightstream.model.{
  AirlineInfo => KAirlineInfo,
  AirplaneInfo => KAirplaneInfo,
  AirportInfo => KAirportInfo,
  FlightEnrichedEvent => KFlightEnrichedEvent,
  GeographyInfo => KGeographyInfo
}
import it.bitrock.kafkageostream.testcommons.FixtureLoanerAnyResult
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafka, EmbeddedKafkaConfig, _}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._

class FlightKafkaConsumerSpec
    extends TestKit(ActorSystem("FlightKafkaConsumerSpec"))
    with EmbeddedKafka
    with BaseSpec
    with Eventually
    with BeforeAndAfterAll
    with TestValues {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(6.seconds)

  "Kafka Consumer" should {

    "forward any record it reads from its subscribed topics to the configured processor, in order" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, kafkaConfig, flightReceivedKeySerde, processorProbe, pollMessages) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val longSerde: Serde[String]            = flightReceivedKeySerde

        val kFlightEnrichedEvent = KFlightEnrichedEvent(
          DefaultIataNumber,
          KGeographyInfo(DefaultLatitude, DefaultLongitude, DefaultAltitude, DefaultDirection),
          DefaultSpeed,
          KAirportInfo(DefaultCodeAirport1, DefaultNameAirport1, DefaultNameCountry1, DefaultCodeIso2Country1),
          KAirportInfo(DefaultCodeAirport2, DefaultNameAirport2, DefaultNameCountry2, DefaultCodeIso2Country2),
          KAirlineInfo(DefaultNameAirline, DefaultSizeAirline),
          Some(KAirplaneInfo(DefaultProductionLine, DefaultModelCode)),
          DefaultStatus,
          DefaultUpdated
        )
        val expectedFlightReceived = FlightReceived(
          DefaultIataNumber,
          GeographyInfo(DefaultLatitude, DefaultLongitude, DefaultAltitude, DefaultDirection),
          DefaultSpeed,
          AirportInfo(DefaultCodeAirport1, DefaultNameAirport1, DefaultNameCountry1, DefaultCodeIso2Country1),
          AirportInfo(DefaultCodeAirport2, DefaultNameAirport2, DefaultNameCountry2, DefaultCodeIso2Country2),
          AirlineInfo(DefaultNameAirline, DefaultSizeAirline),
          Some(AirplaneInfo(DefaultProductionLine, DefaultModelCode)),
          DefaultStatus,
          DefaultUpdated
        )

        withRunningKafka {

          // Using eventually to ignore any warm up time Kafka could have
          eventually {
            publishToKafka(kafkaConfig.flightReceivedTopic, "key", kFlightEnrichedEvent)
            pollMessages()

            processorProbe.expectMsg(expectedFlightReceived)
          }
        }
    }

    "ignore messages present on a topic before Consumer is started" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, kafkaConfig, flightReceivedKeySerde, processorProbe, pollMessages) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val longSerde: Serde[String]            = flightReceivedKeySerde

        val kFlightEnrichedEvent = KFlightEnrichedEvent(
          DefaultIataNumber,
          KGeographyInfo(DefaultLatitude, DefaultLongitude, DefaultAltitude, DefaultDirection),
          DefaultSpeed,
          KAirportInfo(DefaultCodeAirport1, DefaultNameAirport1, DefaultNameCountry1, DefaultCodeIso2Country1),
          KAirportInfo(DefaultCodeAirport2, DefaultNameAirport2, DefaultNameCountry2, DefaultCodeIso2Country2),
          KAirlineInfo(DefaultNameAirline, DefaultSizeAirline),
          Some(KAirplaneInfo(DefaultProductionLine, DefaultModelCode)),
          DefaultStatus,
          DefaultUpdated
        )

        withRunningKafka {
          // Publish to a topic before consumer is started
          publishToKafka(kafkaConfig.flightReceivedTopic, kFlightEnrichedEvent)

          val deadline = patienceConfig.interval.fromNow

          while (deadline.hasTimeLeft) {
            pollMessages()
            processorProbe.expectMsg(NoMessage)
          }

          val (_, response) = consumeFirstKeyedMessageFrom[String, KFlightEnrichedEvent](kafkaConfig.flightReceivedTopic)

          response shouldBe kFlightEnrichedEvent
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
      val flightReceivedKeySerde = Serdes.String
      val processor              = TestProbe()
      val kafkaConsumerWrapper =
        KafkaConsumerWrapperFactory.flightKafkaConsumerFactory(kafkaConfig).build(processor.ref, List(kafkaConfig.flightReceivedTopic))

      try {
        body(
          Resource(
            embKafkaConfig,
            kafkaConfig,
            flightReceivedKeySerde,
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
      flightReceivedKeySerde: Serde[String],
      processorProbe: TestProbe,
      pollMessages: () => Unit
  )
}
