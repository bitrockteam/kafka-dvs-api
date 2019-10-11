package it.bitrock.kafkaflightstream.api.config

import java.net.URI

import akka.http.scaladsl.model.Uri

import scala.concurrent.duration.FiniteDuration

case class KafkaConfig(
    bootstrapServers: String,
    schemaRegistryUrl: URI,
    groupId: String,
    flightReceivedTopic: String,
    flightReceivedListTopic: String,
    topArrivalAirportTopic: String,
    topDepartureAirportTopic: String,
    topSpeedTopic: String,
    topAirlineTopic: String,
    totalFlightTopic: String,
    totalAirlineTopic: String,
    consumer: ConsumerConfig,
    ksql: KsqlConfig
)

final case class ConsumerConfig(
    pollInterval: FiniteDuration,
    startupRewind: FiniteDuration
)

final case class KsqlConfig(
    ksqlServerHost: java.net.URI,
    ksqlPath: String
) {
  def ksqlServerUri: String =
    Uri(ksqlServerHost.resolve(ksqlPath).toString).toString
}
