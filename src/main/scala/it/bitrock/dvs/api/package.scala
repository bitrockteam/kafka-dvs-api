package it.bitrock.dvs

import akka.actor.ActorSystem

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

package object api {
  def duration2JavaDuration(d: FiniteDuration): java.time.Duration =
    java.time.Duration.ofNanos(d.toNanos)

  implicit class ActorSystemOps(actorSystem: ActorSystem) {
    def scheduleOnce(delay: FiniteDuration)(f: => Unit)(implicit executor: ExecutionContext): Unit = {
      actorSystem.scheduler.scheduleOnce(delay)(f)
      ()
    }
  }
}
