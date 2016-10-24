package it.reti.spark.functional

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}






/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
	* Created by gazzopa1 on 19/09/2016.
	*/
case class Topic (name: String, key_words: Seq[String])














/**
	* Created by gazzopa1 on 19/10/2016.
	*/
object Topic {


	/*..................................................................................................................*/
	/**
		* This method provides a DataFrame with a full topic column, mantaining the correct association with
		* their tweet_id.
		*
		* @param hashtagDF a DataFrame with "tweet_id", "text" and "hashtag" infos
		* @param tweetsDF a DataFrame with all tweets infos
		* @return DataFrame which associates each tweet_id with its own topics
		*
		* @note there could be more than one row per tweet_id! there will be one row for each associated topic
		*/
	 def elaborateTopics(hashtagDF: DataFrame, tweetsDF: DataFrame): DataFrame ={


		//sql implicits import
		import hashtagDF.sparkSession.implicits._

		/*
		Check if hashtagDF is not null.
		If so, find topic with full infos accessible.
		Otherwise a new DataFrame with "tweet_id", "text" and "hashtag= null" is passed.
			*/


		val t = Try(hashtagDF.first())

		t match{

			case Success(_)  => TopicHelper.findTopic_hashtag_and_text(hashtagDF)

			case Failure(_)  => TopicHelper.findTopic_text_only(   tweetsDF.select(
				$"tweet_id",
				$"text",
				lit(null: String).as("hashtag")
			)//end select
			)
		}




	}// end elaborateTopics method //
}
