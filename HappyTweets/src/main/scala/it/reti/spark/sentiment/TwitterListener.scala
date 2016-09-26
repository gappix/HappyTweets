package it.reti.spark.sentiment

import twitter4j.Status


trait TwitterListener {
  def onTweet(status: Status): Unit

  def onException(ex: Exception): Unit = {}
}
