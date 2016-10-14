package it.reti.spark.functional

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.util.Try

/**
	* Created by gazzopa1 on 14/10/2016.
	*/
object TweetExtractorHelper {









	/*....................................................................................................................*/
	/**
		* Extracts all localization infos since they're available
		*
		* @param rawTweets: input tweets DataFrame
		* @return  a DataFrame with correct selections
		*/
	def full_select(rawTweets: DataFrame, extractor: String => UserDefinedFunction): DataFrame ={

		//import implicits for DataFrame manipulation
		import rawTweets.sparkSession.implicits._


		rawTweets.select(

			$"id".as("tweet_id"),
			$"lang",
			$"user.id".as("user_id"),
			$"user.name".as("user_name"),

			$"user.screen_name".as("screen_name"),
			when($"in_reply_to_status_id".isNull, false).otherwise(true).as("retweets_something"),

			$"text",
			$"timestamp_ms".as("time"),

			//Geo Localization latitude
			when($"coordinates.coordinates".isNotNull,  extractor("geo_latitude")(rawTweets("coordinates.coordinates")) )   // extract_geo_localization_latitude(rawTweets("coordinates.coordinates")))
				.otherwise(null)
				.as("geo_latitude"),

			//Geo Localization longitude
			when($"coordinates.coordinates".isNotNull, extractor("geo_longitude")(rawTweets("coordinates.coordinates"))   )   // extract_geo_localization_latitude(rawTweets("coordinates.coordinates")))
				.otherwise(null)
				.as("geo_longitude"),

			//Place latitude
			when($"place.bounding_box.coordinates".isNotNull, extractor("bb_latitude")(rawTweets("place.bounding_box.coordinates"))    ) // extract_bounding_box_latitude(rawTweets("place.bounding_box.coordinates")))
				.otherwise(null)
				.as("place_latitude"),

			//Place longitude
			when($"place.bounding_box.coordinates".isNotNull, extractor("bb_longitude")(rawTweets("place.bounding_box.coordinates"))    )//extract_bounding_box_longitude(rawTweets("place.bounding_box.coordinates")))
				.otherwise(null)
				.as("place_longitude"),


			//Region extraction
			when($"place.full_name".isNotNull, extractor("region")(rawTweets("place.full_name"))      ) // extract_region))
				.otherwise(null)
				.as("region")





		)
	}//end full_select helper method //












	/*..................................................................................................................*/
	/**
		* Extracts only coordinates infos since place ones are not available
		*
		* @param rawTweets: input tweets DataFrame
		* @return  a DataFrame with correct selections
		*/
	def coordinates_only_select(rawTweets: DataFrame, extractor: String => UserDefinedFunction): DataFrame ={


		//import implicits for DataFrame manipulation
		import rawTweets.sparkSession.implicits._


		rawTweets.select(

			$"id".as("tweet_id"),
			$"lang",
			$"user.id".as("user_id"),
			$"user.name".as("user_name"),

			$"user.screen_name".as("screen_name"),
			when($"in_reply_to_status_id".isNull, false).otherwise(true).as("retweets_something"),

			$"text",
			$"timestamp_ms".as("time"),

			//Geo Localization latitude
			when($"coordinates.coordinates".isNotNull, extractor("geo_latitude")(rawTweets("coordinates.coordinates"))			)
				.otherwise(null)
				.as("geo_latitude"),

			//Geo Localization longitude
			when($"coordinates.coordinates".isNotNull, extractor("geo_longitude")(rawTweets("coordinates.coordinates"))			)
				.otherwise(null)
				.as("geo_longitude"),

			//Place latitude
			lit(null: String).as("place_latitude"),

			//Place longitude
			lit(null: String).as("place_longitude"),


			//No Region infos
			lit(null: String).as("region")


		)

	}//end coordinates_only_select helper method //











	/*..................................................................................................................*/
	/**
		* Extracts only place infos since coordinates ones are not available
		*
		* @param rawTweets: input tweets DataFrame
		* @return  a DataFrame with correct selections
		*/
	def place_only_select(rawTweets: DataFrame, extractor: String => UserDefinedFunction): DataFrame ={


		//import implicits for DataFrame manipulation
		import rawTweets.sparkSession.implicits._




		rawTweets.select(

			$"id".as("tweet_id"),
			$"lang",
			$"user.id".as("user_id"),
			$"user.name".as("user_name"),

			$"user.screen_name".as("screen_name"),
			when($"in_reply_to_status_id".isNull, false).otherwise(true).as("retweets_something"),

			$"text",
			$"timestamp_ms".as("time"),

			//Geo Localization latitude
			lit(null: String).as("geo_latitude"),

			//Geo Localization longitude
			lit(null: String).as("geo_longitude"),

			//Place latitude
			when($"place.bounding_box.coordinates".isNotNull, extractor("bb_latitude")(rawTweets("place.bounding_box.coordinates"))		)
				.otherwise(null)
				.as("place_latitude"),

			//Place longitude
			when($"place.bounding_box.coordinates".isNotNull, extractor("bb_longitude")(rawTweets("place.bounding_box.coordinates")) 	)
				.otherwise(null)
				.as("place_longitude"),


			//Region extraction
			when($"place.full_name".isNotNull, extractor("region")(rawTweets("place.full_name"))		)
				.otherwise(null)
				.as("region")


		)

	}//end place_only_select helper method //










	/*..................................................................................................................*/
	/**
		* Extracts no coordinates infos since they're not available
		*
		* @param rawTweets: input tweets DataFrame
		* @return  a DataFrame with correct selections
		*/
	def null_select(rawTweets: DataFrame): DataFrame ={



		//import implicits for DataFrame manipulation
		import rawTweets.sparkSession.implicits._





		rawTweets.select(

			$"id".as("tweet_id"),
			$"lang",
			$"user.id".as("user_id"),
			$"user.name".as("user_name"),

			$"user.screen_name".as("screen_name"),
			when($"in_reply_to_status_id".isNull, false).otherwise(true).as("retweets_something"),

			$"text",
			$"timestamp_ms".as("time"),

			//Geo Localization latitude
			lit(null: String).as("geo_latitude"),

			//Geo Localization longitude
			lit(null: String).as("geo_longitude"),

			//Place latitude
			lit(null: String).as("place_latitude"),

			//Place longitude
			lit(null: String).as("place_longitude"),


			//No Region infos
			lit(null: String).as("region")


		)

	}//end null_select helper method //








	/*..................................................................................................................*/
	/**
		* Function wich checks if a specific path exists in param dataframe
		*
		* @param df: DataFrame to be checked
		* @param path: string with the (nested) path to check
		* @return true if it exists, false otherwise
		*/
	def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess







}
