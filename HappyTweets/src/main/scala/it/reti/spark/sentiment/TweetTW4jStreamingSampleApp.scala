package it.reti.spark.sentiment

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.storage.StorageLevel




/**
	* Created by gazzopa1 on 30/09/2016.
	*/
abstract class TweetTW4jStreamingSampleApp(batchInterval: Int) extends TweetTW4JApp("streaming"){
	
	
	def prepareData()
	
	import sqlContextHIVE.implicits._
	
	 def acquireData(): Unit = {
	
				
	
	//interval settings
				val intervalSeconds = 25 /* <---- set this to best interval for your operation */
				
				//import context needed
				val sc = ContextHandler.getSparkContext
				val ssc = new StreamingContext(sc, Seconds(intervalSeconds))
				val sqlContextHIVE = ContextHandler.getSqlContextHIVE
				
				
				/*----------------------------------------------------
				* Twitter Auth Settings
				* >>>>>>> maybe in a .properties file?
				*----------------------------------------------------*/
				
				//twitter auth magic configuration
				val consumerKey = "hARrkNBpwsh8lLldQt7fTe4iM"
				val consumerSecret = "p0BRXCYEePUrJXPHQBxdIkP14idAYaSi934VJU2Hm2LBCUuqg0"
				val accessToken = "72019464-oUaReZ3i91fcVKg3Y7mBOzxlNrNXMpa5sxOcIld3R"
				val accessTokenSecret = "338I4ldbMc3CDpGYrpx5BuDfYbcAAZbJRDW86i9EY6Nwf"
				
				//setting system properties so that Twitter4j library can use general OAuth credentials
				System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
				System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
				System.setProperty("twitter4j.oauth.accessToken", accessToken)
				System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
				System.setProperty("twitter4j.http.proxyHost", "10.1.8.13");
				System.setProperty("twitter4j.http.proxyPort", "8080");
				
				
				
				//val myLocation = new Location(augmentString(locationToObserve).toInt)
				
				//optionally set word filters on input data
				val filters = new Array[String](0)
				
				
				
				/*------------------------------------------------
				 * Stream input and transformations
				 *-----------------------------------------------*/
				
				//input tweets
				/*<<< INFO >>>*/ logInfo("Opening Twitter stream...")
				val streamTweets = TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_ONLY_SER)
				/*<<< INFO >>>*/ logInfo("Stream opened!")
				
				
				
				
				
				
				
				//filtering by language and coordinates
				val englishTweets = streamTweets
					.filter { status => status.getLang=="it"}
					//.filter { status =>  myLocation.checkLocation(status) }
				
				
				
				
				/*----------------------------------------------------
					* Transformations for each RDD
					*---------------------------------------------------*/
				englishTweets.foreachRDD { my_rdd    =>
					
					
					
					/*<<< INFO >>>*/ logInfo(my_rdd.toString() +  " started!")
					
					
					

					
					//checking if there is any message to process
					if(my_rdd.count() > 0){
						
						
						
						
						//Elaborate method invoked at every RDD
						prepareData(my_rdd)
						

						
						
					}//end if
					
					
					
					
					/*<<< INFO >>>*/ logInfo("\n\n ========================================== END ROUND ============================================>>>\n\n\n")
					
					
					
				}//end foreachRDD
				
				
				
				
				
				
				
				/*------------------------------------------------
				 * Streaming start, await and stop
				 *-----------------------------------------------*/
				
				ssc.start()
				ssc.awaitTermination()
				
	
	}//end acquireData overrided method //
	

		

	
	
	
}
