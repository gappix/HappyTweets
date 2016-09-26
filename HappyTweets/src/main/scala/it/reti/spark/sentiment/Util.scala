package it.reti.spark.sentiment

import twitter4j.conf.{Configuration, ConfigurationBuilder}
import org.rogach.scallop.ScallopConf

object Util {
  def config(args: Seq[String]): Configuration = {
    val conf = new ScallopConf(args) {
      val consumerKey = opt[String]("consumer-key", 'k', "Twitter API Consumer Key", required = true)
      val consumerSecret = opt[String]("consumer-secret", 's', "Twitter API Consumer Secret", required = true)
      val accessToken = opt[String]("access-token", 't', "Twitter API Access Token", required = true)
      val accessTokenSecret = opt[String]("access-token-secret", 'e', "Twitter API Access Token Secret", required = true)

      verify()
    }

    new ConfigurationBuilder()
      .setOAuthConsumerKey(conf.consumerKey())
      .setOAuthConsumerSecret(conf.consumerSecret())
      .setOAuthAccessToken(conf.accessToken())
      .setOAuthAccessTokenSecret(conf.accessTokenSecret())
      .build()
  }
}
