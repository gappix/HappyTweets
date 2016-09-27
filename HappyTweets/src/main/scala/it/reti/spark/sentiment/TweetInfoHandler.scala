package it.reti.spark.sentiment



import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.util.Try

/**
	*
	*/
object TweetInfoHandler extends Serializable with Logging{
	

	val sqlContext = ContextHandler.getSqlContext
	import sqlContext.implicits._

	
	/*---------------------------------------------------------------------------------
   * UDF definition: function needed for desired dataframe selection
   *---------------------------------------------------------------------------------*/
	/*
	
	 */


		/** UDF function for unboxing bounding box structure in order to get Place Latitude information */
		private val extract_bounding_box_latitude = udf((box: Seq[Seq[Seq[Double]]] ) => {
			
		box.head.head.last
		
		})// end UDF extract_bounding_box_latitude function //
		
		
		
		
		/** UDF function for unboxing bounding box structure in order to get Place Latitude information */
		private val extract_bounding_box_longitude = udf((box: Seq[Seq[Seq[Double]]]) => {
			
			box.head.head.head
		
		})// end UDF extract_bounding_box_longitude function //
		

		
				
		/** UDF function for unboxing bounding box structure in order to get Place Latitude information */
		private val extract_geo_localization_latitude = udf(( box: Seq[Double])  => {
			
			box.last

		})// end UDF extract_geo_localization_latitude function //
	
	
	
		/** UDF function for unboxing bounding box structure in order to get Place Latitude information */
		private val extract_geo_localization_longitude = udf (( box: Seq[Double]) =>{
		
		box.head
			
		})// end UDF extract_geo_localization_longitude function //
	
	
	
	
		/** UDF function for extract region from full_name field */
		private val extract_region = udf((fullName: String)  =>  {
			
				/*
				The full_name field is composed by: "full_name_of_place, region_name"
				We invoke a split function upon the ", " separator.
				We retrieve second-array element (lowerizing it) as it is the  desired information string
				 */
			
			fullName
				.split(", ")
				.last
				.toLowerCase()

		})//end extract_region UDF function //
	
	
	
	
	/*..................................................................................................................*/
	/**
		* Method to build a DataFrame which contains a full tweet_id -> hashtag association list
		*
		* @param rawTweets
		* @return a DataFrame with 2 columns: "tweet_id", "hashtag", null if no hashtags are available
		* @note there could be multiple "tweet_id" rows whet is is associated to 2 or plus hashtags
		*/
	def selectHashtagData(rawTweets: DataFrame): DataFrame = {
		
		/*
		First a check on the presence of the entities.hashtags infos is done.
		If everything is ok, it is returned a DataFrame with: "tweet_id", "text", "hashtag" fields,
		Otherwise it is returned a null value.
		 */
		if (hasColumn(rawTweets, "entities.hashtags")) {
			
			
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
	
	





	/*..................................................................................................................*/
	/**
		*
		* @param rawTweets
		* @return
		*/
	def selectTweetsData(rawTweets: DataFrame): DataFrame = {
		
		/*
		Coordinates informations:
		if any coordinate value exists, unbox it using udf functions defined above;
		otherwise it will be left null
		
		> BOUNDING BOX: coordinates related to a predefined place (i.e. "Los Angeles, Ca")
		> COORDINATES: accurate tweet geolocation (explicitly enabled by user)
		*/
		
		
		/*
		Check if exists a coordinates.coordinates value with a specific function.
		 */
		

		if (hasColumn(rawTweets, "coordinates.coordinates")) {
			
			if (hasColumn(rawTweets, "place.bounding_box.coordinates")) {
				full_select(rawTweets)
			}
			
			else {
				coordinates_only_select(rawTweets)
			}
		} else if (hasColumn(rawTweets, "place.bounding_box.coordinates")) {
			place_only_select(rawTweets)
		}
		else
			null_select(rawTweets)
		

	}// end selectTweetsData //
	
	
	
	
	
	
	
	/*..................................................................................................................*/
	/**
		* Function wich checks if a specific path exists in param dataframe
		*
		* @param df: DataFrame to be checked
		* @param path: string with the (nested) path to check
		* @return true if it exists, false otherwise
		*/
	private def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

	


	
	
	
	
	/*....................................................................................................................*/
	/**
		* Extracts all localization infos since they're available
		*
		* @param rawTweets: input tweets DataFrame
		* @return  a DataFrame with correct selections
		*/
	private def full_select(rawTweets: DataFrame): DataFrame ={
	
	rawTweets.select(
		
		$"id".as("tweet_id"),
		$"lang",
		$"user.id".as("user_id"),
		$"user.name".as("user_name"),
		$"text",
		$"timestamp_ms".as("time"),
		
		//Geo Localization latitude
		when($"coordinates.coordinates".isNotNull, extract_geo_localization_latitude(rawTweets("coordinates.coordinates")))
			.otherwise(null)
			.as("geo_latitude"),
		
		//Geo Localization longitude
		when($"coordinates.coordinates".isNotNull, extract_geo_localization_latitude(rawTweets("coordinates.coordinates")))
			.otherwise(null)
			.as("geo_longitude"),
		
		//Place latitude
		when($"place.bounding_box.coordinates".isNotNull, extract_bounding_box_latitude(rawTweets("place.bounding_box.coordinates")))
			.otherwise(null)
			.as("place_latitude"),
		
		//Place longitude
		when($"place.bounding_box.coordinates".isNotNull, extract_bounding_box_longitude(rawTweets("place.bounding_box.coordinates")))
			.otherwise(null)
			.as("place_longitude"),
		
		
		//Region extraction
		when($"place.full_name".isNotNull, extract_region(rawTweets("place.full_name")))
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
	private def coordinates_only_select(rawTweets: DataFrame): DataFrame ={
		
		rawTweets.select(
			
			$"id".as("tweet_id"),
			$"lang",
			$"user.id".as("user_id"),
			$"user.name".as("user_name"),
			$"text",
			$"timestamp_ms".as("time"),
			
			//Geo Localization latitude
			when($"coordinates.coordinates".isNotNull, extract_geo_localization_latitude(rawTweets("coordinates.coordinates")))
				.otherwise(null)
				.as("geo_latitude"),
			
			//Geo Localization longitude
			when($"coordinates.coordinates".isNotNull, extract_geo_localization_latitude(rawTweets("coordinates.coordinates")))
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
	private def place_only_select(rawTweets: DataFrame): DataFrame ={
		
		rawTweets.select(
			
			$"id".as("tweet_id"),
			$"lang",
			$"user.id".as("user_id"),
			$"user.name".as("user_name"),
			$"text",
			$"timestamp_ms".as("time"),
			
			//Geo Localization latitude
			lit(null: String).as("geo_latitude"),
			
			//Geo Localization longitude
			lit(null: String).as("geo_longitude"),
			
			//Place latitude
			when($"place.bounding_box.coordinates".isNotNull, extract_bounding_box_latitude(rawTweets("place.bounding_box.coordinates")))
				.otherwise(null)
				.as("place_latitude"),
			
			//Place longitude
			when($"place.bounding_box.coordinates".isNotNull, extract_bounding_box_longitude(rawTweets("place.bounding_box.coordinates")))
				.otherwise(null)
				.as("place_longitude"),
			
			
			//Region extraction
			when($"place.full_name".isNotNull, extract_region(rawTweets("place.full_name")))
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
	private def null_select(rawTweets: DataFrame): DataFrame ={
		
		rawTweets.select(
			
			$"id".as("tweet_id"),
			$"lang",
			$"user.id".as("user_id"),
			$"user.name".as("user_name"),
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
	
	
	
		



}// end  TweetInfoHandler object |||||||||||||||||||||||||||||
