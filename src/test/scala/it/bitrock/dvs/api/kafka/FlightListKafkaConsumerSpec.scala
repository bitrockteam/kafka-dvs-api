package it.bitrock.dvs.api.kafka

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import it.bitrock.dvs.api.config.{AppConfig, KafkaConfig}
import it.bitrock.dvs.api.kafka.FlightListKafkaConsumerSpec.Resource
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper.NoMessage
import it.bitrock.dvs.api.model._
import it.bitrock.dvs.api.{BaseSpec, TestValues}
import it.bitrock.dvs.model.avro.{
  AirlineInfo => KAirlineInfo,
  AirplaneInfo => KAirplaneInfo,
  AirportInfo => KAirportInfo,
  FlightReceived => KFlightReceived,
  FlightReceivedList => KFlightReceivedList,
  GeographyInfo => KGeographyInfo
}
import it.bitrock.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.testcommons.FixtureLoanerAnyResult
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafka, EmbeddedKafkaConfig, _}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._

class FlightListKafkaConsumerSpec
    extends TestKit(ActorSystem("FlightListKafkaConsumerSpec"))
    with EmbeddedKafka
    with BaseSpec
    with Eventually
    with BeforeAndAfterAll
    with TestValues {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(6.seconds)

  "Kafka Consumer" should {
    "forward any record it reads from its subscribed topics to the configured processor, in order" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, kafkaConfig, flightListReceivedKeySerde, processorProbe, pollMessages) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val stringSerde: Serde[String]          = flightListReceivedKeySerde

        val kFlightReceivedEvent1 = KFlightReceived(
          DefaultIataNumber,
          DefaultIcaoNumber,
          KGeographyInfo(DefaultLatitude, DefaultLongitude, DefaultAltitude, DefaultDirection),
          DefaultSpeed1,
          KAirportInfo(
            DefaultCodeAirport1,
            DefaultNameAirport1,
            DefaultLatitude1,
            DefaultLongitude1,
            DefaultNameCountry1,
            DefaultCodeIso2Country1,
            DefaultTimezone1,
            DefaultGmt1
          ),
          KAirportInfo(
            DefaultCodeAirport2,
            DefaultNameAirport2,
            DefaultLatitude2,
            DefaultLongitude2,
            DefaultNameCountry2,
            DefaultCodeIso2Country2,
            DefaultTimezone2,
            DefaultGmt2
          ),
          KAirlineInfo(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
          KAirplaneInfo(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
          DefaultStatus,
          DefaultUpdated
        )
        val kFlightReceivedEvent2 = KFlightReceived(
          DefaultIataNumber,
          DefaultIcaoNumber,
          KGeographyInfo(DefaultLatitude, DefaultLongitude, DefaultAltitude, DefaultDirection),
          DefaultSpeed2,
          KAirportInfo(
            DefaultCodeAirport1,
            DefaultNameAirport1,
            DefaultLatitude1,
            DefaultLongitude1,
            DefaultNameCountry1,
            DefaultCodeIso2Country1,
            DefaultTimezone1,
            DefaultGmt1
          ),
          KAirportInfo(
            DefaultCodeAirport2,
            DefaultNameAirport2,
            DefaultLatitude2,
            DefaultLongitude2,
            DefaultNameCountry2,
            DefaultCodeIso2Country2,
            DefaultTimezone2,
            DefaultGmt2
          ),
          KAirlineInfo(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
          KAirplaneInfo(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
          DefaultStatus,
          DefaultUpdated
        )
        val kFlightReceivedList = KFlightReceivedList(Seq(kFlightReceivedEvent1, kFlightReceivedEvent2))
        val expectedFlightReceived1 = FlightReceived(
          DefaultIataNumber,
          DefaultIcaoNumber,
          Geography(DefaultLatitude, DefaultLongitude, DefaultAltitude, DefaultDirection),
          DefaultSpeed1,
          Airport(
            DefaultCodeAirport1,
            DefaultNameAirport1,
            DefaultNameCountry1,
            DefaultCodeIso2Country1,
            DefaultTimezone1,
            DefaultLatitude1,
            DefaultLongitude1,
            DefaultGmt1
          ),
          Airport(
            DefaultCodeAirport2,
            DefaultNameAirport2,
            DefaultNameCountry2,
            DefaultCodeIso2Country2,
            DefaultTimezone2,
            DefaultLatitude2,
            DefaultLongitude2,
            DefaultGmt2
          ),
          Airline(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
          Airplane(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
          DefaultStatus,
          DefaultUpdated.toEpochMilli
        )
        val expectedFlightReceived2 = FlightReceived(
          DefaultIataNumber,
          DefaultIcaoNumber,
          Geography(DefaultLatitude, DefaultLongitude, DefaultAltitude, DefaultDirection),
          DefaultSpeed2,
          Airport(
            DefaultCodeAirport1,
            DefaultNameAirport1,
            DefaultNameCountry1,
            DefaultCodeIso2Country1,
            DefaultTimezone1,
            DefaultLatitude1,
            DefaultLongitude1,
            DefaultGmt1
          ),
          Airport(
            DefaultCodeAirport2,
            DefaultNameAirport2,
            DefaultNameCountry2,
            DefaultCodeIso2Country2,
            DefaultTimezone2,
            DefaultLatitude2,
            DefaultLongitude2,
            DefaultGmt2
          ),
          Airline(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
          Airplane(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
          DefaultStatus,
          DefaultUpdated.toEpochMilli
        )
        val expectedFlightReceivedList = FlightReceivedList(Seq(expectedFlightReceived1, expectedFlightReceived2))

        withRunningKafka {

          // Using eventually to ignore any warm up time Kafka could have
          eventually {
            publishToKafka(kafkaConfig.flightReceivedListTopic, "key", kFlightReceivedList)
            pollMessages()

            processorProbe.expectMsg(expectedFlightReceivedList)
          }
        }
    }

    "ignore messages present on a topic before Consumer is started" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, kafkaConfig, flightListReceivedKeySerde, processorProbe, pollMessages) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val longSerde: Serde[String]            = flightListReceivedKeySerde

        val kFlightReceivedEvent = KFlightReceived(
          DefaultIataNumber,
          DefaultIcaoNumber,
          KGeographyInfo(DefaultLatitude, DefaultLongitude, DefaultAltitude, DefaultDirection),
          DefaultSpeed1,
          KAirportInfo(
            DefaultCodeAirport1,
            DefaultNameAirport1,
            DefaultLatitude,
            DefaultLongitude,
            DefaultNameCountry1,
            DefaultCodeIso2Country1,
            DefaultTimezone1,
            DefaultGmt1
          ),
          KAirportInfo(
            DefaultCodeAirport2,
            DefaultNameAirport2,
            DefaultLatitude,
            DefaultLongitude,
            DefaultNameCountry2,
            DefaultCodeIso2Country2,
            DefaultTimezone2,
            DefaultGmt2
          ),
          KAirlineInfo(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
          KAirplaneInfo(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
          DefaultStatus,
          DefaultUpdated
        )
        val kFlightReceivedList = KFlightReceivedList(Seq(kFlightReceivedEvent))

        withRunningKafka {
          // Publish to a topic before consumer is started
          publishToKafka(kafkaConfig.flightReceivedListTopic, "key", kFlightReceivedList)

          eventually {
            pollMessages()
            processorProbe.expectMsg(NoMessage)
          }

          val (_, response) =
            consumeFirstKeyedMessageFrom[String, KFlightReceivedList](kafkaConfig.flightReceivedListTopic)

          response shouldBe kFlightReceivedList
        }
    }
  }

  override def afterAll: Unit = {
    shutdown()
    super.afterAll()
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
      val flightListReceivedKeySerde = Serdes.String
      val processor                  = TestProbe()
      val kafkaConsumerWrapper =
        KafkaConsumerWrapperFactory
          .flightListKafkaConsumerFactory(kafkaConfig)
          .build(processor.ref, List(kafkaConfig.flightReceivedListTopic))

      try {
        body(
          Resource(
            embKafkaConfig,
            kafkaConfig,
            flightListReceivedKeySerde,
            processor,
            () => kafkaConsumerWrapper.pollMessages()
          )
        )
      } finally {
        kafkaConsumerWrapper.close()
      }
    }
  }

}

object FlightListKafkaConsumerSpec {

  final case class Resource(
      embeddedKafkaConfig: EmbeddedKafkaConfig,
      kafkaConfig: KafkaConfig,
      flightListReceivedKeySerde: Serde[String],
      processorProbe: TestProbe,
      pollMessages: () => Unit
  )
}
