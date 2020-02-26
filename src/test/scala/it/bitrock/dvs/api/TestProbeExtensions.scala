package it.bitrock.dvs.api

import akka.testkit.TestProbe

import scala.concurrent.duration.FiniteDuration

object TestProbeExtensions {
  implicit class TestKitOps(tk: TestProbe) {
    def expectMessage[T](obj: T)(implicit timeout: FiniteDuration): T = tk.expectMsg(timeout, obj)
  }
}
