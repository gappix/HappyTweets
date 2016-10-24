package it.reti.spark.functional

import org.apache.spark.sql._

/**
	* Created by gazzopa1 on 19/10/2016.
	*/
object SentimentHelper {



	//def get_udf_function()




	/*..................................................................................................................*/
	/**
		* This method counts, for each tweet_id, how many words its text is formed by
		*
		* @param inputDF: DataFrame with following fields: "tweet_id", "word" (already sanitized)
		* @return a DataFrame with two fields: "tweet_id", "tweet_words"
		*/
	def count_tweets_words(inputDF: DataFrame): DataFrame = {


		//count original tweet words
		inputDF.groupBy("tweet_id")
			.agg( "word"-> "count" )
			.withColumnRenamed("count(word)","tweet_words")


	}//end method //

}
