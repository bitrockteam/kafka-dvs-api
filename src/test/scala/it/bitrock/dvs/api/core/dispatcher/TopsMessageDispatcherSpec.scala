package it.bitrock.dvs.api.core.dispatcher

import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit._
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import it.bitrock.dvs.api.core.poller.TopsKafkaPollerCache
import it.bitrock.dvs.api.model._
import spray.json._

class TopsMessageDispatcherSpec extends BaseTestKit {

  "Tops Message Dispatcher" should {

    "forward a JSON to source actor" when {
      "a TopArrivalAirportList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher =
            MessageDispatcherFactory
              .topsMessageDispatcherFactory(topsKafkaPollerCache, webSocketConfig)
              .build(sourceProbe.ref)
          val msg = TopArrivalAirportList(List(AirportCount(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount)))
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
      "a TopDepartureAirportList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher =
            MessageDispatcherFactory
              .topsMessageDispatcherFactory(topsKafkaPollerCache, webSocketConfig)
              .build(sourceProbe.ref)
          val msg =
            TopDepartureAirportList(List(AirportCount(DefaultDepartureAirport1Name, DefaultDepartureAirport1Amount)))
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
      "a TopSpeedList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher =
            MessageDispatcherFactory
              .topsMessageDispatcherFactory(topsKafkaPollerCache, webSocketConfig)
              .build(sourceProbe.ref)
          val msg = TopSpeedList(List(SpeedFlight(DefaultFlightCode1, DefaultSpeed)))
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
      "a TopAirlineList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher =
            MessageDispatcherFactory
              .topsMessageDispatcherFactory(topsKafkaPollerCache, webSocketConfig)
              .build(sourceProbe.ref)
          val msg = TopAirlineList(List(AirlineCount(DefaultAirline1Name, DefaultAirline1Amount)))
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
    }
  }

}
