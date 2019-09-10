package it.bitrock.kafkaflightstream.api.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.{Http, HttpExt}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future

trait HttpClientFactory {
  def singleRequest(httpRequest: HttpRequest): Future[HttpResponse]

  def close(): Future[Unit]
}

class HttpClientFactoryImpl(implicit system: ActorSystem) extends HttpClientFactory with LazyLogging {

  private val httpClient: HttpExt = Http()(system)

  override def singleRequest(httpRequest: HttpRequest): Future[HttpResponse] = httpClient.singleRequest(httpRequest)

  override def close(): Future[Unit] = httpClient.shutdownAllConnectionPools()

}
