package it.reti.spark.sentiment

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame


/*||||||||||||||||||||||||||||||||||||||||||||||||    TWEET JSON APP   |||||||||||||||||||||||||||||||||||||||||||||||*/
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
	override def prepareData(rawTweets: DataFrame): Unit = {
				
		
				//select  needed fields using an helper
				val cleanedTweets = TweetInfoHandler.selectTweetsData(rawTweets)
				val hashtagTweets= TweetInfoHandler.selectHashtagData(rawTweets)


				
				
				
				//launch the core elaborator
				runElaborator(cleanedTweets, hashtagTweets)
				
				
			}// end prepareJsonData method //
	
	
	
	
	
	
	
}//end TweetJSONAPP class |||||||||||||||

