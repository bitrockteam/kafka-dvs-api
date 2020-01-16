package it.bitrock.dvs.api.core.dispatcher

import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.core.factory.TopsMessageDispatcherFactoryImpl
import it.bitrock.dvs.api.core.poller.TopsKafkaPollerCache
import it.bitrock.dvs.api.definitions._
import spray.json._
import it.bitrock.dvs.api.BaseTestKit._

class TopsMessageDispatcherSpec extends BaseTestKit {

  "Tops Message Dispatcher" should {

    "forward a JSON to source actor" when {
      "a TopArrivalAirportList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(websocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher    = new TopsMessageDispatcherFactoryImpl(websocketConfig, topsKafkaPollerCache).build(sourceProbe.ref)
          val msg                  = TopArrivalAirportList(Seq(Airport(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount)))
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
      "a TopDepartureAirportList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(websocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher    = new TopsMessageDispatcherFactoryImpl(websocketConfig, topsKafkaPollerCache).build(sourceProbe.ref)
          val msg                  = TopDepartureAirportList(Seq(Airport(DefaultDepartureAirport1Name, DefaultDepartureAirport1Amount)))
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
      "a TopSpeedList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(websocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher    = new TopsMessageDispatcherFactoryImpl(websocketConfig, topsKafkaPollerCache).build(sourceProbe.ref)
          val msg                  = TopSpeedList(Seq(SpeedFlight(DefaultFlightCode1, DefaultSpeed)))
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
      "a TopAirlineList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(websocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher    = new TopsMessageDispatcherFactoryImpl(websocketConfig, topsKafkaPollerCache).build(sourceProbe.ref)
          val msg                  = TopAirlineList(Seq(Airline(DefaultAirline1Name, DefaultAirline1Amount)))
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
    }
  }

}
