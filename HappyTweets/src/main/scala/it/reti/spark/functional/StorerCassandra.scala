package it.reti.spark.functional

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
	* Created by gazzopa1 on 19/10/2016.
	*/
object StorerCassandra {



	/*..................................................................................................................*/
	/**
		*
		* @param tweets
		*/
	def store_tweets(tweets: DataFrame, processingMode: String) = {

		val keyspace = AppSettings.cassandra_keyspace
		val table = "event_processed_" + processingMode

		tweets.write.format("org.apache.spark.sql.cassandra").option("table",table).option("keyspace",keyspace).mode(SaveMode.Append).save()


	}//end store_tweets method//



	/*..................................................................................................................*/
	/**
		*
		* @param hashtags
		* @param processingMode
		*/
	def store_hashtags(hashtags: DataFrame, processingMode: String) ={

		val keyspace = AppSettings.cassandra_keyspace
		val table = "event_hashtag_"  + processingMode

		hashtags.write.format("org.apache.spark.sql.cassandra").option("table",table).option("keyspace",keyspace).mode(SaveMode.Append).save()


	}//end store_hashtag method //




	/*..................................................................................................................*/
	/**
		*
		* @param sentiment
		* @param processingMode
		*/
	def store_sentiment(sentiment: DataFrame, processingMode: String) ={

		val keyspace = AppSettings.cassandra_keyspace
		val table = "event_sentiment_" + processingMode

		sentiment.write.format("org.apache.spark.sql.cassandra").option("table",table).option("keyspace",keyspace).mode(SaveMode.Append).save()


	}//end store_sentiment method //



	/*..................................................................................................................*/
	/**
		*
		* @param topics
		* @param processingMode
		*/
	def store_topic(topics: DataFrame, processingMode: String) = {

		val keyspace = AppSettings.cassandra_keyspace
		val table = "event_topic_"  + processingMode

		topics.write.format("org.apache.spark.sql.cassandra").option("table",table).option("keyspace",keyspace).mode(SaveMode.Append).save()


	}//end store_topic method //







}//end StorerCassandra object |||||||||||||||||||||||||||||||||||||||
