package it.bitrock.kafkaflightstream.api.services

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpHeader, HttpResponse}
import akka.testkit.TestKit
import it.bitrock.kafkaflightstream.api.client.ksqlserver.definitions._
import it.bitrock.kafkaflightstream.api.client.ksqlserver.ksql.RunKsqlStatementsResponse
import it.bitrock.kafkaflightstream.api.routes.definitions.{CleanSessionRequest, ErrorDescription}
import it.bitrock.kafkaflightstream.api.routes.session.SessionResource.{
  cleanSessionResponse,
  cleanSessionResponseInternalServerError,
  cleanSessionResponseNoContent
}
import it.bitrock.kafkaflightstream.api.services.models.ErrorMessages._
import it.bitrock.kafkaflightstream.api.services.models.KsqlStatements._
import it.bitrock.kafkaflightstream.api.{BaseAsyncSpec, TestValues}
import it.bitrock.kafkageostream.testcommons.AsyncFixtureLoaner
import org.scalatest.{Assertion, BeforeAndAfterAll}

import scala.concurrent.Future

class SessionServiceSpec extends TestKit(ActorSystem("SessionServiceSpec")) with BaseAsyncSpec with BeforeAndAfterAll {

  import SessionServiceSpec._

  "Session Service" should {

    "return NoContent when cleaning a session" in ResourceLoaner.withFixture {
      case Resource(sessionService) =>
        sessionService
          .cleanSession(cleanSessionResponse)(CleanSessionRequest(IndexedSeq(DefaultSink1, DefaultSink2)))
          .map { _ shouldBe cleanSessionResponseNoContent }
    }

    "return NoContent for an empty input" in ResourceLoaner.withFixture {
      case Resource(sessionService) =>
        sessionService
          .cleanSession(cleanSessionResponse)(CleanSessionRequest(IndexedSeq()))
          .map { _ shouldBe cleanSessionResponseNoContent }
    }

    "return InternalServerError if no stream match with input" in ResourceLoaner.withFixture {
      case Resource(sessionService) =>
        val streamsIds = IndexedSeq(DefaultNotFoundedStreamId)
        sessionService
          .cleanSession(cleanSessionResponse)(CleanSessionRequest(streamsIds))
          .map { _ shouldBe cleanSessionResponseInternalServerError(ErrorDescription(noMatchFound(streamsIds.map(_.toUpperCase): _*))) }
    }

  }

  object ResourceLoaner extends AsyncFixtureLoaner[Resource] {
    override def withFixture(body: Resource => Future[Assertion]): Future[Assertion] = {

      val ksqlClientFactory = new TestKsqlClientFactoryImpl
      val ksqlOps           = new KsqlOpsImpl(ksqlClientFactory)
      val sessionService    = new SessionService(ksqlOps)

      body(Resource(sessionService))

    }
  }

  override def afterAll: Unit = {
    shutdown()
    super.afterAll()
  }

}

object SessionServiceSpec extends TestValues {

  final case class Resource(sessionService: SessionService)

  class TestKsqlClientFactoryImpl extends KsqlClientFactory {

    final val TerminateQueryStatement = terminateQuery(DefaultQueryId1, DefaultQueryId2)
    final val DropStreamStatement     = dropStreamDeleteTopic(DefaultSink1, DefaultSink2)

    def runKsqlStatements(
        body: RunStatements,
        headers: List[HttpHeader]
    ): Future[Either[Either[Throwable, HttpResponse], RunKsqlStatementsResponse]] =
      Future.successful {
        body.ksql match {
          case ShowQueries =>
            Right(
              RunKsqlStatementsResponse.OK(
                IndexedSeq(
                  queries(
                    IndexedSeq(
                      Query(DefaultQueryId1, IndexedSeq(DefaultSink1)),
                      Query(DefaultQueryId2, IndexedSeq(DefaultSink2)),
                      Query(DefaultQueryId3, IndexedSeq(DefaultSink3))
                    )
                  )
                )
              )
            )
          case TerminateQueryStatement =>
            Right(
              RunKsqlStatementsResponse.OK(
                IndexedSeq(
                  currentStatus(s"terminate/$DefaultQueryId1/execute", CommandStatus("SUCCESS", "")),
                  currentStatus(s"terminate/$DefaultQueryId2/execute", CommandStatus("SUCCESS", ""))
                )
              )
            )
          case DropStreamStatement =>
            Right(
              RunKsqlStatementsResponse.OK(
                IndexedSeq(
                  currentStatus(s"stream/$DefaultSink1/drop", CommandStatus("SUCCESS", "")),
                  currentStatus(s"stream/$DefaultSink2/drop", CommandStatus("SUCCESS", ""))
                )
              )
            )
        }
      }

  }

}
