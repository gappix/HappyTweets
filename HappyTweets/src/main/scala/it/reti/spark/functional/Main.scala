package it.reti.spark.functional

import org.apache.log4j.{Level, Logger, LogManager}
import org.apache.spark.sql.SparkSession


//|||||||||||||||||||||||||||||||||||||||||||||||    MAIN    |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
/**
 *  Main object for the SPARK Twitter Analyzer app
 */  
object Main {




  /*.................................................................................................................*/
  /**
    * main method definition
    * @param args "batch" or "streaming + #batch-seconds" string to select desired execution mode
    */
  def main(args: Array[String]) {


		//app logger global settings
		@transient lazy  val log = LogManager.getLogger("myLogger")
		Logger.getRootLogger.setLevel(Level.ERROR)
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		Logger.getLogger("it.reti.spark").setLevel(Level.INFO)




		//print app title
		AppSettings.print_title






		//spark session and context creation
		val spark = SparkSession.builder()
			.config(AppSettings.spark_configuration)
			.getOrCreate()

		import spark.implicits._





		/*.............................................*
			INPUT
		 *.............................................*/



		//input sources
		val rawTweetsDF = spark.read.json(AppSettings.input_file).persist()
		val sentix = spark.read.text(AppSettings.sentix_file).as[String]



		//extract a clean Sentix dataframe and broadcast it
		val cleanedSentix = Sentix.build_dictionary(sentix)
		val broadcast_sentix = spark.sparkContext.broadcast(cleanedSentix)







		/*.............................................*
			ELABORATION
		 *.............................................*/

		//Tweet infos extraction
		val selectedTweetsDF = TweetExtractor.tweet_clean_info(rawTweetsDF).persist()


		//Tweets hashtag extraction
		val hashtagDF = HashtagExtractor.extract_hashtag_data(rawTweetsDF).persist()


		//Tweets sentiment evaluation
		val sentimentDF = Sentiment.evaluate_sentiment(selectedTweetsDF, broadcast_sentix.value).persist()


		//Topic finder
		val topicsDF = Topic.elaborateTopics(hashtagDF, selectedTweetsDF).persist()







		/*.............................................*
			STORING
		 *.............................................*/

		/*<<INFO>>*/log.info("Elaborating tweets...")/*<<INFO>>*/
		selectedTweetsDF.show()
		/*<<INFO>>*/log.info("Tweets elaborated! >>>>  Now saving to Cassandra... ")/*<<INFO>>*/
		//StorerCassandra.store_topics(selectedTweetsDF)
		/*<<INFO>>*/  log.info("Tweets storing completed!") /*<<INFO>>*/



		/*<<INFO>>*/log.info("Elaborating sentiment...")/*<<INFO>>*/
		sentimentDF.show()
		/*<<INFO>>*/log.info("Sentiment elaborated! >>>>  Now saving to Cassandra...")/*<<INFO>>*/
		//StorerCassandra.store_topics(sentimentDF)
		/*<<INFO>>*/  log.info("Sentiment storing completed!")/*<<INFO>>*/




		/*<<INFO>>*/log.info("Elaborating hashtags...")/*<<INFO>>*/
		hashtagDF.show()
		/*<<INFO>>*/log.info("Hashtag elaborated! >>>>  Now saving to Cassandra... ")/*<<INFO>>*/
		//StorerCassandra.store_topics(hashtagDF)
		/*<<INFO>>*/  log.info("Hashtag storing completed!") /*<<INFO>>*/



		/*<<INFO>>*/log.info("Evaluating topics...")/*<<INFO>>*/
		topicsDF.show()
		/*<<INFO>>*/log.info("Topics evaluated! >>>>  Now saving to Cassandra...")/*<<INFO>>*/
		//StorerCassandra.store_topics(topicsDF)
		/*<<INFO>>*/  log.info("Topics storing completed!") /*<<INFO>>*/









		//<<<<<<<<<<<< MEMORY FREE <<<<<<<<<<<<<<<  |  |  |  |  |
		hashtagDF.unpersist()
		sentimentDF.unpersist()
		rawTweetsDF.unpersist()
		topicsDF.unpersist()
		//<<<<<<<<<<<< MEMORY FREE <<<<<<<<<<<<<<<  |  |  |  |  |








	}//end main method //

  
  
}//end main object |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||