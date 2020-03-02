package it.bitrock.dvs.api

import akka.testkit.TestProbe

object TestProbeExtensions {
  import scala.concurrent.duration.FiniteDuration

  implicit class TestKitOps(tk: TestProbe) {
    def expectMessage[T](obj: T)(implicit timeout: FiniteDuration): T = tk.expectMsg(timeout, obj)
  }
}
