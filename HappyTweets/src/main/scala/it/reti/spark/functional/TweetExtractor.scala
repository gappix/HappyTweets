package it.reti.spark.functional

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

/**
	* Created by gazzopa1 on 13/10/2016.
	*/
object TweetExtractor {


	/**
		*
		* @param rawTweetsDF
		* @return
		*/
	def tweetProcessAndClean( rawTweetsDF: DataFrame): Dataset[Tweet] = {


		/*---------------------------------------------------------------------------------
			 * UDF definition: function needed for desired dataframe selection
			 *---------------------------------------------------------------------------------*/


		/** UDF function for unboxing bounding box structure in order to get Place Latitude information */
		val extract_bounding_box_latitude = udf((box: Seq[Seq[Seq[Double]]] ) => {

			box.head.head.last

		})// end UDF extract_bounding_box_latitude function //






		/** UDF function for unboxing bounding box structure in order to get Place Latitude information */
		val extract_bounding_box_longitude = udf((box: Seq[Seq[Seq[Double]]]) => {

			box.head.head.head

		})// end UDF extract_bounding_box_longitude function //






		/** UDF function for unboxing bounding box structure in order to get Place Latitude information */
		val extract_geo_localization_latitude = udf(( box: Seq[Double])  => {

			box.last

		})// end UDF extract_geo_localization_latitude function //





		/** UDF function for unboxing bounding box structure in order to get Place Latitude information */
		private val extract_geo_localization_longitude = udf (( box: Seq[Double]) =>{

			box.head

		})// end UDF extract_geo_localization_longitude function //




		/** UDF function for extract region from full_name field */
		val extract_region = udf((fullName: String)  =>  {

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






		


	}//end tweetProcessAndClean method //


}





case class Tweet(tweet_id: Long)