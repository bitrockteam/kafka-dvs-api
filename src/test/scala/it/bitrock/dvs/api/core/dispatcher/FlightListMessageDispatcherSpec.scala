package it.bitrock.dvs.api.core.dispatcher

import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.core.factory.FlightListMessageDispatcherFactoryImpl
import it.bitrock.dvs.api.core.poller.FlightListKafkaPollerCache
import it.bitrock.dvs.api.definitions._
import spray.json._

class FlightListMessageDispatcherSpec extends BaseTestKit {

  "Flight List Message Dispatcher" should {

    "forward a JSON to source actor" when {
      "a correct FlightReceivedList message is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(websocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageProcessor =
            new FlightListMessageDispatcherFactoryImpl(websocketConfig, flightListKafkaPollerCache).build(sourceProbe.ref)
          val msg = FlightReceivedList(
            Seq(
              FlightReceived(
                DefaultIataNumber,
                DefaultIcaoNumber,
                GeographyInfo(DefaultInBoxLatitude, DefaultInBoxLongitude, DefaultAltitude, DefaultDirection),
                DefaultSpeed,
                AirportInfo(
                  DefaultCodeAirport1,
                  DefaultNameAirport1,
                  DefaultNameCountry1,
                  DefaultCodeIso2Country1,
                  DefaultTimezone1,
                  DefaultGmt1
                ),
                AirportInfo(
                  DefaultCodeAirport2,
                  DefaultNameAirport2,
                  DefaultNameCountry2,
                  DefaultCodeIso2Country2,
                  DefaultTimezone2,
                  DefaultGmt2
                ),
                AirlineInfo(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
                AirplaneInfo(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
                DefaultStatus,
                DefaultUpdated.toEpochMilli
              )
            )
          )
          messageProcessor ! msg
          sourceProbe expectMsg msg.toJson.toString
      }
      "the flights in the list are inside the box after its change" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(websocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageProcessor =
            new FlightListMessageDispatcherFactoryImpl(websocketConfig, flightListKafkaPollerCache).build(sourceProbe.ref)
          val msg = FlightReceivedList(
            Seq(
              FlightReceived(
                DefaultIataNumber,
                DefaultIcaoNumber,
                GeographyInfo(DefaultChangedInBoxLatitude, DefaultChangedInBoxLongitude, DefaultAltitude, DefaultDirection),
                DefaultSpeed,
                AirportInfo(
                  DefaultCodeAirport1,
                  DefaultNameAirport1,
                  DefaultNameCountry1,
                  DefaultCodeIso2Country1,
                  DefaultTimezone1,
                  DefaultGmt1
                ),
                AirportInfo(
                  DefaultCodeAirport2,
                  DefaultNameAirport2,
                  DefaultNameCountry2,
                  DefaultCodeIso2Country2,
                  DefaultTimezone2,
                  DefaultGmt2
                ),
                AirlineInfo(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
                AirplaneInfo(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
                DefaultStatus,
                DefaultUpdated.toEpochMilli
              )
            )
          )
          messageProcessor ! changedBox
          messageProcessor ! msg
          sourceProbe expectMsg msg.toJson.toString
      }
    }

    "forward an empty message to source actor" when {
      "the flights in the list are out of the box" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(websocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageProcessor =
            new FlightListMessageDispatcherFactoryImpl(websocketConfig, flightListKafkaPollerCache).build(sourceProbe.ref)
          messageProcessor ! FlightReceivedList(
            Seq(
              FlightReceived(
                DefaultIataNumber,
                DefaultIcaoNumber,
                GeographyInfo(DefaultOutBoxLatitude, DefaultOutBoxLongitude, DefaultAltitude, DefaultDirection),
                DefaultSpeed,
                AirportInfo(
                  DefaultCodeAirport1,
                  DefaultNameAirport1,
                  DefaultNameCountry1,
                  DefaultCodeIso2Country1,
                  DefaultTimezone1,
                  DefaultGmt1
                ),
                AirportInfo(
                  DefaultCodeAirport2,
                  DefaultNameAirport2,
                  DefaultNameCountry2,
                  DefaultCodeIso2Country2,
                  DefaultTimezone2,
                  DefaultGmt2
                ),
                AirlineInfo(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
                AirplaneInfo(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
                DefaultStatus,
                DefaultUpdated.toEpochMilli
              )
            )
          )
          sourceProbe expectMsg FlightReceivedList(Seq()).toJson.toString
      }
    }

  }

}
