package it.reti.spark.sentiment

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j._
import twitter4j.conf.Configuration


object TwitterReceiver {
  def sample(config: Configuration, lang: String) = new TwitterReceiver(config) {
    override def factory(config: Configuration): TwitterStream = TwitterStream.sample(config, lang)
  }

  def filter(config: Configuration, filterQuery: FilterQuery) = new TwitterReceiver(config) {
    override def factory(config: Configuration): TwitterStream = TwitterStream.filter(config, filterQuery)
  }
}

abstract class TwitterReceiver(config: Configuration) extends Receiver[Status](StorageLevel.MEMORY_ONLY) {
  private var stream: TwitterStream = _
  private var listener: TwitterListener = _

  def factory(config: Configuration): TwitterStream

  override def onStart(): Unit = {
    stream = factory(config)
    listener = new Listener

    stream.addListener(listener)
    stream.start()
  }

  override def onStop(): Unit = {
    stream.shutdown()
  }

  private class Listener extends TwitterListener {
    override def onTweet(status: Status): Unit = {
      store(status)
    }

    override def onException(ex: Exception): Unit = {}
  }

}
