package it.reti.spark.sentiment



import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}


import scala.util.Try


/**
	* Created by gazzopa1 on 20/09/2016.
	*/
object TweetInfoHandler extends Serializable with Logging {
	
	
	val sqlContext = ContextHandler.getSqlContext
	import sqlContext.implicits._
	
	
	/*---------------------------------------------------------------------------------
   * UDF definition: function needed for desired dataframe selection
   *---------------------------------------------------------------------------------*/
	/*
	
	 */



		/** function for unboxing bounding box structure in order to get Place Latitude information */
		private val extract_bounding_box_latitude = udf((box: Seq[Seq[Seq[Double]]] ) => {
			
		box.head.head.last
		
		
		})// end UDF extract_bounding_box_latitude function //
		
		
		
		
		/** function for unboxing bounding box structure in order to get Place Latitude information */
		private val extract_bounding_box_longitude = udf((box: Seq[Seq[Seq[Double]]]) => {
		
			
			box.head.head.head
		
		})// end UDF extract_bounding_box_longitude function //
		
		
		
		
		
		
		
		
		/* functioor unboxing bounding box structure in order to get Place Latitude information */
		private val extract_geo_localization_latitude = udf(( box: Seq[Double])  => {
			
			box.last

		})
	
	
		/** function for unboxing bounding box structure in order to get Place Latitude information */
		private val extract_geo_localization_longitude = udf (( box: Seq[Double]) =>{
		
		box.head
			
		})
	
	
	
	/**----------------------------------------------------------------------------------------*/
	def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
	
	
	
/*
private val extract_hashtag_list = udf((hashtagSequence: Seq[Row])  =>  {
	 
			var hashtagList = ""
			for(hashtag <- hashtagSequence) {
				
				/*
				hashtag is a Row object with two fields:
				position(0): INDICES explains where to find hashtag in tweet text
				position(1): TEXT in the hashtag true word
				
				We retrieve string in position 1 to get back each hashtag word
				*/
				hashtagList += hashtag.getString(1) + " "
			}
				
				
			hashtagList
	
})//end extract_hashtag_list UDF function //
	*/





/*..................................................................................................................*/
/**
	*
	* @param rawTweets
	* @return
	*/
def selectTweetsData(rawTweets: DataFrame): DataFrame = {
	
	
		if(hasColumn(rawTweets, "coordinates.coordinates"))
	 rawTweets.select(
														/*
														Tweets basic infos:
														"tweet_id", "lang", "user_id", "user_name", "text"
														 */
			$"id".as("tweet_id"),
			$"lang",
			$"user.id".as("user_id"),
			$"user.name".as("user_name"),
			$"text",
		  $"coordinates.coordinates"
	 )
	else
			rawTweets.select(
				
				$"lang",
				$"user.id"
			)

	
	
	//-----------------------------------------------------------
	
	/*
	SOL1 come era all'origine
	 */
	/*
	
	when($"coordinates.coordinates".isNotNull, extract_geo_localization_latitude(rawTweets("coordinates.coordinates"))).as("gl_latitude")
	 */
	
	
	//when($"coordinates.coordinates".isNotNull, extract_geo_localization_latitude(rawTweets("coordinates.coordinates"))).as("gl_latitude")
	
	
	//---------------------------------------------------------------
	
	/*
	SOL2
	 */
	/*
	when($"coordinates.coordinates".isNull, null).otherwise(extract_geo_localization_latitude(rawTweets("coordinates.coordinates"))).as("gl_latitude")
	 */
	
	
	
	
	
	
	// -----------------------------------------------------------------
	
	/*
	SOL 3
	
	cambiare il filtro di tweepy con le coordinate italliane
	
			
	*/
	
	
	
	
	
	/*
	Coordinates informations:
	if any coordinate value exists, unbox it using udf functions defined above;
	otherwise it will be left null
	
	> BOUNDING BOX: coordinates related to a predefined place (i.e. "Los Angeles, Ca")
	> COORDINATES: accurate tweet geolocation (explicitly enabled by user)
	 */
		

		/*
		
		//place latitude
		when($"place".isNull, null)
			  .otherwise(   when($"place.bounding_box".isNull, null)
				    .otherwise(   when($"place.bounding_box.coordinates".isNull, null)
					      .otherwise(   extract_bounding_box_latitude(rawTweets("place.bounding_box.coordinates"))  )
				    )
			  ).as("place_latitude"),
		
		
		//place longitude
		when($"place".isNull, null)
			  .otherwise(when($"place.bounding_box".isNull, null)
				    .otherwise(when($"place.bounding_box.coordinates".isNull, null)
					      .otherwise(extract_bounding_box_longitude(rawTweets("place.bounding_box.coordinates")))
				    )
			  ).as("place_longitude"),
	
				

		
		//geo latitude
		when($"coordinates"., null)
			  .otherwise(   when($"coordinates.coordinates".isNull, null)
				    .otherwise(  extract_geo_localization_latitude(rawTweets("coordinates.coordinates"))    )
			  ).as("geo_latitude"),
		
		//geo longitude
		when($"coordinates".isNull, null)
			  .otherwise(   when($"coordinates.coordinates".isNull, null)
				    .otherwise(   extract_geo_localization_longitude(rawTweets("coordinates.coordinates"))   )
			  ).as("geo_longitude")

		
		*/
		
	
	
	
	// end select
	
	
}// end selectTweetsData method //

	






}// end  TweetInfoHandler object |||||||||||||||||||||||||||||
