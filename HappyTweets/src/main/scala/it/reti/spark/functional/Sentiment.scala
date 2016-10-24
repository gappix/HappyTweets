package it.reti.spark.functional

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._

/**
	* Created by gazzopa1 on 19/10/2016.
	*/
object Sentiment {



	def evaluate_sentiment(inputTweetsDF: DataFrame, sentixDF: DataFrame) : DataFrame = {




		//sql implicits import
		import inputTweetsDF.sparkSession.implicits._




		/*------------------------------------------------*
		 * UDF definitions
		 *------------------------------------------------*/



		/** UDF function to evaluate the confidency value as (matched_words / total_tweet_words) */
		val confidencyValue = udf( (matched_words: Double, tweet_words: Double) =>{  matched_words/tweet_words  })





		/** UDF function to extract a clean word from a generic already-split string*/
		val sanitize_english_tweets = udf (( word: String) =>{

			val regularExpression = "\\w+(\'\\w+)?".r



			val sanitizedWord = regularExpression.findFirstIn(word.toLowerCase)

			sanitizedWord match
			{
				case None            => null
				case Some(something) => something     }

		})// end UDF sanitize_english_tweets function //






		/** UDF function to extract a clean word from a generic already-split string*/
		val sanitize_italian_tweets = udf (( word: String) =>{

			val regularExpression = "\\w+".r
			val sanitizedWord = regularExpression.findAllIn(word.toLowerCase).toList

			if (sanitizedWord.nonEmpty)
				sanitizedWord.last
			else null


		})// end UDF sanitize_italian_tweets function //










		/*---------------------------------------------------*
		 * Tweet words sanitization
		 *---------------------------------------------------*/



		/*
		explode  text(n-words)(1-row) field in
						 word(1-word)(n-rows) one
		 */
		val explodedTWEETS = inputTweetsDF.select($"tweet_id", $"lang", $"text")
			.explode("text", "word"){
				text: String => text.split(" ")
			}





		//sanitize words by udf
		val sanitizedTWEETS = explodedTWEETS.select(
			$"tweet_id",
			when($"lang".equalTo("en"), sanitize_english_tweets(explodedTWEETS("word")))
				.otherwise(sanitize_italian_tweets(explodedTWEETS("word")))
				.as("word")
		)//end select
			.filter(not(isnull($"word")))







		/*---------------------------------------------------*
		 * Sentiment evaluation
		 *---------------------------------------------------*/

		//count tweets words in a new DataFrame
		val wordCountByTweetDF = SentimentHelper.count_tweets_words(sanitizedTWEETS)




		//joining tweets with Hedonometer dictionary
		val sentimentTWEETS = sentixDF.join(sanitizedTWEETS, sentixDF("sentix_word") === sanitizedTWEETS("word"), "inner")
			.groupBy("tweet_id")
			.agg( "absolute_sentiment"  -> "avg",
				"word"                -> "count" )
			.withColumnRenamed("avg(absolute_sentiment)","sentiment_value")
			.withColumnRenamed("count(word)","matched_words")


		//pack results into a new DataFrame
		val sentimentConfidencyTWEETS = sentimentTWEETS.join( wordCountByTweetDF, "tweet_id")
			.select(  $"tweet_id",
				$"sentiment_value",
				$"matched_words",
				$"tweet_words",
				confidencyValue($"matched_words", $"tweet_words").as("confidency_value")
			)

		sentimentConfidencyTWEETS

	}//end elaborateSentiment method //


}
