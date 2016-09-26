package it.reti.spark.sentiment

import twitter4j._
import twitter4j.conf.Configuration


object TwitterStream {
  def sample(config: Configuration, lang: String): TwitterStream = {
    new TwitterStreamImpl(config) {
      override def start(): Unit = sample(lang)
    }
  }

  def filter(config: Configuration, filterQuery: FilterQuery): TwitterStream = {
    new TwitterStreamImpl(config) {
      override def start(): Unit = filter(filterQuery)
    }
  }


  private abstract class TwitterStreamImpl(config: Configuration) extends TwitterStream {
    private val stream = new TwitterStreamFactory(config).getInstance()

    override def addListener(listener: TwitterListener): Unit = {
      stream.addListener(new StatusListener {
        override def onStallWarning(warning: StallWarning): Unit = {}

        override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}

        override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}

        override def onStatus(status: Status): Unit = {
          listener.onTweet(status)
        }

        override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}

        override def onException(ex: Exception): Unit = {
          listener.onException(ex)
        }
      })
    }

    override def stop(): Unit = stream.cleanUp()

    override def shutdown(): Unit = {
      stream.cleanUp()
      stream.shutdown()
    }

    protected def filter(filterQuery: FilterQuery): Unit = {
      stream.filter(filterQuery)
    }

    protected def sample(lang: String): Unit = {
      stream.sample(lang)
    }
  }

}

abstract class TwitterStream {
  def addListener(listener: TwitterListener): Unit

  def start(): Unit

  def stop(): Unit

  def shutdown(): Unit
}
