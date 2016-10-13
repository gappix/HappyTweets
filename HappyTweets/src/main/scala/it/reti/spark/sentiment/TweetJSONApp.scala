package it.reti.spark.sentiment


import org.apache.spark.sql.{DataFrame, SparkSession}


/*||||||||||||||||||||||||||||||||||||||||    TWEET JSON APP   |||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
	* Abstract class which prepares input DataFrames using methods specific for Json-input-red data.
	* It must be extended by elaborator class which acquires data from JSon Twitter API (e.g., python tweepy)
	*
	* @note super class' acquireData method is still mantained abstract: it must be implemented by final specific class.
	*
	* Created by gazzopa1 on 26/09/2016.
	*/
abstract class TweetJSONApp(processingType: String) extends TweetApp(processingType) {
	
	
	
	
	
	/*.................................................................................................................*/
	/**
		* Method to prepare input data DataFrame from a JSON tweet input
		* @param rawTweets: DataFrame coming from a json-input-read
		*/
	def prepareData(rawTweets: DataFrame, spark: SparkSession): Unit = {
				
		
				//select needed fields using an helper
				val cleanedTweets = TweetInfoHandler(spark).selectTweetsData(rawTweets)
				val hashtagTweets= TweetInfoHandler(spark).selectHashtagData(rawTweets)


		cleanedTweets.show(25)
				
				
				
				//launch the core elaborator
				//runElaborator(cleanedTweets, hashtagTweets, spark)
				
				
			}// end prepareJsonData method //
	
	
	
	
	
	
	
}//end TweetJSONAPP class |||||||||||||||

