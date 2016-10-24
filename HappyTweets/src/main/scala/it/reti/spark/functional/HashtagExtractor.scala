package it.reti.spark.functional

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


/*|||||||||||||||||||||||||||||||||||||||||		HASHTAG EXTRACTOR		|||||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
	* Created by gazzopa1 on 18/10/2016.
	*/
object HashtagExtractor {







	/*..................................................................................................................*/
	/**
		* Method to build a DataFrame which contains a full tweet_id -> hashtag association list
		*
		* @param rawTweets
		* @return a DataFrame with 2 columns: "tweet_id", "hashtag", null if no hashtags are available
		* @note there could be multiple "tweet_id" rows whet is is associated to 2 or plus hashtags
		*/
	def extract_hashtag_data(rawTweets: DataFrame): DataFrame = {


		//import implicits from current Session
		import rawTweets.sparkSession.implicits._

		/*
		First a check on the presence of the entities.hashtags infos is performed.
		If everything is ok, it is returned a DataFrame with: "tweet_id", "text", "hashtag" fields,
		Otherwise it is returned a null value.
		 */
		if (TweetExtractorHelper.has_column(rawTweets, "entities.hashtags")) {


					val explodedHashtagDF = rawTweets.select(
						$"id".as("tweet_id"),
						$"text",
						explode($"entities.hashtags").as("exploded_hashtags")
					)//end select


					explodedHashtagDF.select(
						$"tweet_id",
						$"text",
						lower($"exploded_hashtags.text").as("hashtag")
					)//end select

		}

		else{

					//creating an empty DataFrame
					Seq.empty[(Long, String, String)].toDF("tweet_id", "text", "hashtag")
		}


	}//end selectHashtagData method //








}//end Hashtag Extractor object ||||||||||||||||||||||||||
