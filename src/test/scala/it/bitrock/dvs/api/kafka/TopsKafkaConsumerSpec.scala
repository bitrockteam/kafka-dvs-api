package it.bitrock.dvs.api.kafka

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import it.bitrock.dvs.api.config.{AppConfig, KafkaConfig}
import it.bitrock.dvs.api.definitions._
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper.NoMessage
import it.bitrock.dvs.api.{BaseSpec, TestValues}
import it.bitrock.dvs.model.avro.{
  Airline => KAirline,
  Airport => KAirport,
  SpeedFlight => KSpeedFlight,
  TopAirlineList => KTopAirlineList,
  TopArrivalAirportList => KTopArrivalAirportList,
  TopDepartureAirportList => KTopDepartureAirportList,
  TopSpeedList => KTopSpeedList
}
import it.bitrock.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.testcommons.FixtureLoanerAnyResult
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafka, EmbeddedKafkaConfig, _}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._

class TopsKafkaConsumerSpec
    extends TestKit(ActorSystem("TopsKafkaConsumerSpec"))
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

        val kTopArrivalAirportList = KTopArrivalAirportList(
          Seq(
            KAirport(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount),
            KAirport(DefaultArrivalAirport2Name, DefaultArrivalAirport2Amount),
            KAirport(DefaultArrivalAirport3Name, DefaultArrivalAirport3Amount),
            KAirport(DefaultArrivalAirport4Name, DefaultArrivalAirport4Amount),
            KAirport(DefaultArrivalAirport5Name, DefaultArrivalAirport5Amount)
          )
        )
        val kTopDepartureAirportList = KTopDepartureAirportList(
          Seq(
            KAirport(DefaultDepartureAirport1Name, DefaultDepartureAirport1Amount),
            KAirport(DefaultDepartureAirport2Name, DefaultDepartureAirport2Amount),
            KAirport(DefaultDepartureAirport3Name, DefaultDepartureAirport3Amount),
            KAirport(DefaultDepartureAirport4Name, DefaultDepartureAirport4Amount),
            KAirport(DefaultDepartureAirport5Name, DefaultDepartureAirport5Amount)
          )
        )
        val kTopSpeedList = KTopSpeedList(
          Seq(
            KSpeedFlight(DefaultFlightCode1, DefaultSpeed1),
            KSpeedFlight(DefaultFlightCode2, DefaultSpeed2),
            KSpeedFlight(DefaultFlightCode3, DefaultSpeed3),
            KSpeedFlight(DefaultFlightCode4, DefaultSpeed4),
            KSpeedFlight(DefaultFlightCode5, DefaultSpeed5)
          )
        )
        val kTopAirlineList = KTopAirlineList(
          Seq(
            KAirline(DefaultAirline1Name, DefaultAirline1Amount),
            KAirline(DefaultAirline2Name, DefaultAirline2Amount),
            KAirline(DefaultAirline3Name, DefaultAirline3Amount),
            KAirline(DefaultAirline4Name, DefaultAirline4Amount),
            KAirline(DefaultAirline5Name, DefaultAirline5Amount)
          )
        )

        withRunningKafka {
          eventually {
            publishToKafka(kafkaConfig.topArrivalAirportTopic, kTopArrivalAirportList)
            val expectedTopArrivalAirportList = TopArrivalAirportList(
              Seq(
                Airport(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount),
                Airport(DefaultArrivalAirport2Name, DefaultArrivalAirport2Amount),
                Airport(DefaultArrivalAirport3Name, DefaultArrivalAirport3Amount),
                Airport(DefaultArrivalAirport4Name, DefaultArrivalAirport4Amount),
                Airport(DefaultArrivalAirport5Name, DefaultArrivalAirport5Amount)
              )
            )
            pollMessages()
            processorProbe.expectMsg(expectedTopArrivalAirportList)
          }
          eventually {
            publishToKafka(kafkaConfig.topDepartureAirportTopic, kTopDepartureAirportList)
            val expectedTopDepartureAirportList = TopDepartureAirportList(
              Seq(
                Airport(DefaultDepartureAirport1Name, DefaultDepartureAirport1Amount),
                Airport(DefaultDepartureAirport2Name, DefaultDepartureAirport2Amount),
                Airport(DefaultDepartureAirport3Name, DefaultDepartureAirport3Amount),
                Airport(DefaultDepartureAirport4Name, DefaultDepartureAirport4Amount),
                Airport(DefaultDepartureAirport5Name, DefaultDepartureAirport5Amount)
              )
            )
            pollMessages()
            processorProbe.expectMsg(expectedTopDepartureAirportList)
          }
          eventually {
            publishToKafka(kafkaConfig.topSpeedTopic, kTopSpeedList)
            val expectedTopSpeedList = TopSpeedList(
              Seq(
                SpeedFlight(DefaultFlightCode1, DefaultSpeed1),
                SpeedFlight(DefaultFlightCode2, DefaultSpeed2),
                SpeedFlight(DefaultFlightCode3, DefaultSpeed3),
                SpeedFlight(DefaultFlightCode4, DefaultSpeed4),
                SpeedFlight(DefaultFlightCode5, DefaultSpeed5)
              )
            )
            pollMessages()
            processorProbe.expectMsg(expectedTopSpeedList)
          }
          eventually {
            publishToKafka(kafkaConfig.topAirlineTopic, kTopAirlineList)
            val expectedTopAirlineList = TopAirlineList(
              Seq(
                Airline(DefaultAirline1Name, DefaultAirline1Amount),
                Airline(DefaultAirline2Name, DefaultAirline2Amount),
                Airline(DefaultAirline3Name, DefaultAirline3Amount),
                Airline(DefaultAirline4Name, DefaultAirline4Amount),
                Airline(DefaultAirline5Name, DefaultAirline5Amount)
              )
            )
            pollMessages()
            processorProbe.expectMsg(expectedTopAirlineList)
          }
        }

    }

    "ignore messages present on a topic before Consumer is started" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, kafkaConfig, topKeySerde, processorProbe, pollMessages) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val stringSerde: Serde[String]          = topKeySerde

        val kTopArrivalAirportList = KTopArrivalAirportList(
          Seq(
            KAirport(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount),
            KAirport(DefaultArrivalAirport2Name, DefaultArrivalAirport2Amount),
            KAirport(DefaultArrivalAirport3Name, DefaultArrivalAirport3Amount),
            KAirport(DefaultArrivalAirport4Name, DefaultArrivalAirport4Amount),
            KAirport(DefaultArrivalAirport5Name, DefaultArrivalAirport5Amount)
          )
        )
        val kTopDepartureAirportList = KTopDepartureAirportList(
          Seq(
            KAirport(DefaultDepartureAirport1Name, DefaultDepartureAirport1Amount),
            KAirport(DefaultDepartureAirport2Name, DefaultDepartureAirport2Amount),
            KAirport(DefaultDepartureAirport3Name, DefaultDepartureAirport3Amount),
            KAirport(DefaultDepartureAirport4Name, DefaultDepartureAirport4Amount),
            KAirport(DefaultDepartureAirport5Name, DefaultDepartureAirport5Amount)
          )
        )
        val kTopSpeedList = KTopSpeedList(
          Seq(
            KSpeedFlight(DefaultFlightCode1, DefaultSpeed1),
            KSpeedFlight(DefaultFlightCode2, DefaultSpeed2),
            KSpeedFlight(DefaultFlightCode3, DefaultSpeed3),
            KSpeedFlight(DefaultFlightCode4, DefaultSpeed4),
            KSpeedFlight(DefaultFlightCode5, DefaultSpeed5)
          )
        )
        val kTopAirlineList = KTopAirlineList(
          Seq(
            KAirline(DefaultAirline1Name, DefaultAirline1Amount),
            KAirline(DefaultAirline2Name, DefaultAirline2Amount),
            KAirline(DefaultAirline3Name, DefaultAirline3Amount),
            KAirline(DefaultAirline4Name, DefaultAirline4Amount),
            KAirline(DefaultAirline5Name, DefaultAirline5Amount)
          )
        )

        withRunningKafka {
          // Publish to a topic before consumer is started
          publishToKafka(kafkaConfig.topArrivalAirportTopic, kTopArrivalAirportList)
          publishToKafka(kafkaConfig.topDepartureAirportTopic, kTopDepartureAirportList)
          publishToKafka(kafkaConfig.topSpeedTopic, kTopSpeedList)
          publishToKafka(kafkaConfig.topAirlineTopic, kTopAirlineList)

          val deadline = patienceConfig.interval.fromNow

          while (deadline.hasTimeLeft) {
            pollMessages()
            processorProbe.expectMsg(NoMessage)
          }

          val (_, responseArrivalAirport) =
            consumeFirstKeyedMessageFrom[String, KTopArrivalAirportList](kafkaConfig.topArrivalAirportTopic)
          responseArrivalAirport shouldBe kTopArrivalAirportList

          val (_, responseDepartureAirport) =
            consumeFirstKeyedMessageFrom[String, KTopDepartureAirportList](kafkaConfig.topDepartureAirportTopic)
          responseDepartureAirport shouldBe kTopDepartureAirportList

          val (_, responseSpeed) =
            consumeFirstKeyedMessageFrom[String, KTopSpeedList](kafkaConfig.topSpeedTopic)
          responseSpeed shouldBe kTopSpeedList

          val (_, responseAirline) =
            consumeFirstKeyedMessageFrom[String, KTopAirlineList](kafkaConfig.topAirlineTopic)
          responseAirline shouldBe kTopAirlineList

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
      val topKeySerde: Serde[String] = Serdes.String
      val processor                  = TestProbe()
      val kafkaConsumerWrapper = KafkaConsumerWrapperFactory
        .topsKafkaConsumerFactory(kafkaConfig)
        .build(
          processor.ref,
          List(
            kafkaConfig.topArrivalAirportTopic,
            kafkaConfig.topDepartureAirportTopic,
            kafkaConfig.topSpeedTopic,
            kafkaConfig.topAirlineTopic
          )
        )

      try {
        body(
          Resource(
            embKafkaConfig,
            kafkaConfig,
            topKeySerde,
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
      topKeySerde: Serde[String],
      processorProbe: TestProbe,
      pollMessages: () => Unit
  )
}
