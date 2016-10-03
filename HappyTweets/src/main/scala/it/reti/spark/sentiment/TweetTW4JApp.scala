package it.reti.spark.sentiment

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import twitter4j.Status



/**
	* Created by gazzopa1 on 30/09/2016.
	*/
abstract class TweetTW4JApp(processingType: String) extends TweetApp(processingType)  {
	
	
	import sqlContextHIVE.implicits._
	
	/*.................................................................................................................*/
	/**
		* Method to prepare input data DataFrame from a Twitter4j.Status tweet input
		* @param my_rdd: DataFrame coming from a twitter4j-input-read
		*/
	def prepareData(my_rdd: RDD[Status]): Unit = {





/*...........................................
	* DataFrame schema definition
	*.........................................*/

val schema = StructType(
													Array( StructField("tweet_id",      LongType,   true),
																 StructField("lang",          StringType, true),
																 
																 StructField("user_id",       LongType,   true),
																 StructField("user_name",     StringType, true),
																 
																 StructField("geo_latitude",   DoubleType, true),
																 StructField("geo_longitude",  DoubleType, true),
																 
																 StructField("place_latitude",   DoubleType, true),
																 StructField("place_longitude",  DoubleType, true),
																 
																 StructField("text",          StringType, true),
																 StructField("time",          LongType,   true),


																	StructField("region", StringType, true)
																 
																)//end of Array definition
												 
																
												)//end of StructType




/*...........................................
* DataFrame definition
*.........................................*/
	
	
//ready tweet with correct info extraction
val readyTweetsDF = sqlContextHIVE.createDataFrame(
																										my_rdd.map(
																		
																														status =>   Row(
																																							status.getId,                         //tweet_id
																																							status.getLang,                       //lang
																																							
																																							status.getUser.getId,                 //user_id
																																							status.getUser.getName,               //user_name
																																							
																																							getGeoLocationCoordinates(status)._1, //gl_latitude
																																							getGeoLocationCoordinates(status)._2, //gl_longitude
																																							
																																							getPlaceCoordinates(status.getPlace)._1, //bb_latitude
																																							getPlaceCoordinates(status.getPlace)._2, //bb_longitude
																																							
																																							status.getText,                       //text
																																							status.getCreatedAt.getTime, //data
																															
																																							status.getHashtagEntities
																																										.map( x => x.getText)
																																										.mkString(" ")
																																										.toLowerCase()// hastag list separated by " "
																																							)
																																							
																														 ), schema)//end of createDataFrame
		
		
		
	}
	
	
	
	
	
	
	
	/*.................................................................................................................*/
	/**
		* Method that
		* @return (Place latitude, Place longitude) if present, (None None) otherwise
		*/
	def getPlaceCoordinates(place : twitter4j.Place) : (Option[Double], Option[Double], String) = {
		
		
		
		
		
		if (place == null)  (None, None, "place null")
		
		else {
			
			val boundingBoxCoordinates  = place.getBoundingBoxCoordinates
			val geometryCoordinates     = place.getGeometryCoordinates
			val containerPlace          = place.getContainedWithIn
			
			//check Bounding Box Coordinates
			if (boundingBoxCoordinates != null) (  Some(boundingBoxCoordinates.head.head.getLatitude),  Some(boundingBoxCoordinates.head.head.getLongitude),   "bounding box")
			else {
				
				//check Geometry Coordinates
				if (geometryCoordinates != null)  (  Some(geometryCoordinates.head.head.getLatitude),  Some(geometryCoordinates.head.head.getLongitude),   "geometry")
				else{
					
					//check Container Place
					if (containerPlace != null) getPlaceCoordinates(containerPlace.head)
					else (None, None, "everything is null")
				}
			}
		}
		
		
	}// end getPlaceCoordinates
	
	
	
	
	/*.................................................................................................................*/
	/**
		* Method that
		* @return (GeoLocation latitude, GeoLocation longitude) if present, (None None) otherwise
		*/
	def getGeoLocationCoordinates( status : twitter4j.Status) : (Option[Double], Option[Double]) = {
		
		
		status.getGeoLocation match{
			
			case null => (None, None)
			case default => (Some(status.getGeoLocation.getLatitude),Some(status.getGeoLocation.getLongitude))
			
		}
		
		
	}// end getGeoLocationCoordinates method //
	
	
	
	
	
	/*.................................................................................................................*/
	/**
		* Method that
		* @return (Place latitude, Place longitude) if present, (None None) otherwise
		*/
	def getBoundingBoxCoordinates(status : twitter4j.Status) : (Double, Double) = {
		
		
		if (status != null && status.getPlace != null && status.getPlace.getBoundingBoxCoordinates != null) {
			return (status.getPlace.getBoundingBoxCoordinates.head.head.getLatitude, status.getPlace.getBoundingBoxCoordinates.head.head.getLongitude)
		}
		else {
			return (null.asInstanceOf[Double], null.asInstanceOf[Double])
		}
		
		
	}// end getBoundingBoxCoordinates //
		
		
		
	}//end class ||||||||||||||||||||||||||||||||
