package it.reti.spark.functional

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

import scala.util.Try

/**
	* Created by gazzopa1 on 13/10/2016.
	*/
object TweetExtractor {


	/**
		*
		* @param rawTweetsDF
		* @return
		*/
	def tweet_clean_info ( rawTweetsDF: DataFrame): DataFrame = {




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


		if (TweetExtractorHelper.has_column(rawTweetsDF, "coordinates.coordinates")) {

			if (TweetExtractorHelper.has_column(rawTweetsDF, "place.bounding_box.coordinates")) {
				TweetExtractorHelper.full_select(rawTweetsDF, extractor)
			}

			else {
				TweetExtractorHelper.coordinates_only_select(rawTweetsDF, extractor)
			}
		} else if (TweetExtractorHelper.has_column(rawTweetsDF, "place.bounding_box.coordinates")) {
			TweetExtractorHelper.place_only_select(rawTweetsDF, extractor)
		}
		else
			TweetExtractorHelper.null_select(rawTweetsDF)


	}// end selectTweetsData //













	/*..................................................................................................................*/
	/**
		*
		* @param field_to_extract
		* @return
		*/
	def extractor(field_to_extract: String ):  UserDefinedFunction = {


			field_to_extract match {

						case ("geo_latitude") 	=>   	udf(( box: Seq[Double])  						=> 		box.last )
						case ("geo_longitude")	=>		udf (( box: Seq[Double]) 						=>  	box.head )
						case ("bb_latitude") 		=>		udf((box: Seq[Seq[Seq[Double]]] ) 	=> 		box.head.head.last )
						case ("bb_longitude")		=>		udf((box: Seq[Seq[Seq[Double]]]) 		=> 		box.head.head.head )
						case ("region") 				=>		udf((fullName: String) 							=>  {

																						/*
																						The full_name field is composed by: "full_name_of_place, region_name"
																						We invoke a split function upon the ", " separator.
																						We retrieve second-array element (lowerizing it) as it is the  desired information string
																						 */
																						fullName
																							.split(", ")
																							.last
																							.toLowerCase()

																					})
						case _ 								=> 	udf(() 		=>  null )

			}//end match case



	}//end extractor method //








}





case class Tweet(tweet_id: Long)