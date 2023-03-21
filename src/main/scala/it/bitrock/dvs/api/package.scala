package it.bitrock.dvs

import akka.actor.{ActorSystem, Cancellable}
import cats.Semigroup
import cats.syntax.all._
import it.bitrock.dvs.api.model.{FlightReceived, Precedence}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

package object api {
  implicit private lazy val booleanSemigroup: Semigroup[Boolean] = (x: Boolean, y: Boolean) => x && y

  def duration2JavaDuration(d: FiniteDuration): java.time.Duration =
    java.time.Duration.ofNanos(d.toNanos)

  implicit class ActorSystemOps(actorSystem: ActorSystem) {
    def scheduleOnce(delay: FiniteDuration)(f: => Unit)(implicit executor: ExecutionContext): Unit = {
      actorSystem.scheduler.scheduleOnce(delay)(f)
      ()
    }

    def scheduleEvery(delay: FiniteDuration)(f: => Unit)(implicit executor: ExecutionContext): Cancellable =
      actorSystem.scheduler.scheduleWithFixedDelay(Duration.Zero, delay)(() => f)
  }

  implicit class FlightFieldToPrecedence(f: FlightReceived) {
    def hasPrecedence(precedences: List[Precedence]): Boolean = precedences.exists(hasPrecedence)

    def hasPrecedence(precedence: Precedence): Boolean = {
      val arrivalAirport: Option[Boolean]   = precedence.arrivalAirport.map(_ == f.airportArrival.codeAirport)
      val departureAirport: Option[Boolean] = precedence.departureAirport.map(_ == f.airportDeparture.codeAirport)
      val airline: Option[Boolean]          = precedence.airline.map(_ == f.airline.codeAirline)

      (arrivalAirport |+| departureAirport |+| airline).getOrElse(false)

    }
  }
}
