package it.bitrock.kafkaflightstream.api.services.models

object KsqlStatements {

  type KsqlStatement = String

  final val ShowQueries: KsqlStatement = "SHOW QUERIES;"

  def terminateQuery(queryIds: String*): KsqlStatement = queryIds.map(id => s"TERMINATE $id;").mkString(" ")

  def dropStreamDeleteTopic(streamIds: String*): KsqlStatement = streamIds.map(id => s"DROP STREAM $id DELETE TOPIC;").mkString(" ")

}
