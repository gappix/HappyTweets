package horizon

import org.apache.spark.sql.{DataFrame, SparkSession}


/*||||||||||||||||||||||||||||||||||||||||    TWEET JSON APP   |||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
	* Abstract class which prepares input DataFrames using methods specific for Json-input-red data.
	* It must be extended by a specific class which acquires data from a JSON Twitter API source (e.g., python tweepy)
	*
	* @note super class' acquireData method is still mantained abstract: it must be implemented by final specific class.
	*
	* Created by gazzopa1 on 26/09/2016.
	*/
abstract class TweetJSONApp(processingType: String)  {

	 //<-----------------------------
	
	def acquireData()// <-------------------------------

	
	/*.................................................................................................................*/
	/**
		* Method to prepare input data DataFrame from a JSON tweet input
		* @param rawTweets: DataFrame coming from a json-input-read
		*/
	def prepareData(rawTweets: DataFrame): Unit = {

		@transient lazy  val log = org.apache.log4j.LogManager.getLogger("horizon")


		//keep the scope on active SparkSession
		import rawTweets.sparkSession.implicits._

		
				//select needed fields using an helper
				val cleanedTweets = JSONInfoHandler.selectTweetsData(rawTweets)
				//val hashtagTweets= TweetInfoHandler.selectHashtagData(rawTweets)


				rawTweets.select($"id").show(5)
				log.info(rawTweets.sparkSession.toString)
				
				
				
				//launch the core elaborator
				//runElaborator(cleanedTweets, hashtagTweets, spark)
				
				
			}// end prepareJsonData method //
	
	
	
	
	
	
	
}//end TweetJSONAPP class |||||||||||||||

