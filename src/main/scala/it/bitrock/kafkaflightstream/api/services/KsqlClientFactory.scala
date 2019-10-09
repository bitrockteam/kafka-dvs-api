package it.bitrock.kafkaflightstream.api.services

import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.api.client.ksqlserver.definitions.RunStatements
import it.bitrock.kafkaflightstream.api.client.ksqlserver.ksql.{KsqlClient, RunKsqlStatementsResponse}
import it.bitrock.kafkaflightstream.api.routes.HttpClientFactory

import scala.concurrent.{ExecutionContext, Future}

trait KsqlClientFactory {

  def runKsqlStatements(
      body: RunStatements,
      headers: List[HttpHeader] = Nil
  ): Future[Either[Either[Throwable, HttpResponse], RunKsqlStatementsResponse]]

}

class KsqlClientFactoryImpl(httpClient: HttpClientFactory)(implicit ec: ExecutionContext, materializer: ActorMaterializer)
    extends KsqlClientFactory
    with LazyLogging {

  private val ksqlClient = KsqlClient.httpClient(httpClient.singleRequest)

  def runKsqlStatements(
      body: RunStatements,
      headers: List[HttpHeader]
  ): Future[Either[Either[Throwable, HttpResponse], RunKsqlStatementsResponse]] =
    ksqlClient.runKsqlStatements(body, headers).value

}
