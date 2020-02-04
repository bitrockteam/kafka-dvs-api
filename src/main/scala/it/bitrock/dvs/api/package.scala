package it.bitrock.dvs

import akka.actor.{ActorSystem, Cancellable}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

package object api {
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
}
