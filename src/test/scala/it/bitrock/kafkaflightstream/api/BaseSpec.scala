package it.bitrock.kafkaflightstream.api

import it.bitrock.kafkageostream.testcommons.{AsyncSuite, Suite}
import org.scalatest.{AsyncWordSpecLike, WordSpecLike}

/**
  * Base trait to test classes.
  */
trait BaseSpec extends Suite with WordSpecLike

/**
  * Base trait to test asynchronous assertions.
  */
trait BaseAsyncSpec extends AsyncSuite with AsyncWordSpecLike
