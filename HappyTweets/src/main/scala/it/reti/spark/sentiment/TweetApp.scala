package it.reti.spark.sentiment

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.Logging



/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
 *   This abstract class contains all main methods for processing execution.
 *   It must be extended implementing the Run method according to desired execution.
 *   Elaboration and storing methods are, on the contrary, common for every purpose.   
 */
abstract class TweetApp(processingType : String) extends Serializable with Logging{

  
  
  
  //data storer object
  val myDataStorer = new DataStorer(processingType)
  
  
  
  //get sqlHIVE context and import methods for DataFrame/RDD conversion
  val sqlContextHIVE = ContextHandler.getSqlContextHIVE
  import sqlContextHIVE.implicits._
	
	
	

	
	
	
	
	
	/*.................................................................................................................*/
  /** 
  * Method TRAIT.
  * MUST BE OVERRIDED in class extension according to selected input source.
  */
  def acquireData()
  
  
  
	
	
  
  
  
  /*..................................................................................................................*/
	/**
		*
		* @param rawTweets
		*/
	def prepareJsonData(rawTweets: DataFrame) = {
		
		

		
		
		

		
		//select  needed fields using an helper
		
		val cleanedTweets = TweetInfoHandler.selectTweetsData(rawTweets)
		cleanedTweets.show(600,false)
		cleanedTweets.printSchema()
		
		
		/*<<< INFO >>>*/ logInfo("Received " + cleanedTweets.count().toString + " tweets!")
		
		
		
		//launch the core elaborator@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
		//runElaborator(cleanedTweets)
		
		
	}// end prepareJsonData method //
	
	
	
	
  
  
  
  /*.................................................................................................................*/
  /**
   * Method which elaborates tweet DataFrames evaluating sentiment values, hashtags and topics.
   *
   * 
   * @param rawTweetsDF	DataFrame with all tweet potentially useful fields
   */
  def runElaborator(rawTweetsDF : DataFrame)  = {
    
    
    /*
    It  joins tweets with an "Hedonometer dictionary" which assign to each word an happiness value.
    All values are then averaged obtaining an approximate sentiment value for each tweet message.
    
     */
  
  
  

                               
    /*.............................................*
     * Original DataFrame transformations
     *.............................................*/
                               
    /*
     Selecting desired fields:
     if available, take GeoLocation infos (which are most accurate)
     otherwise take Place.BoundingBox ones
     
     This DataFrame is ready to be stored in "tweets_processed" table
     */
    val readyTweetsDF = rawTweetsDF.select(
                                            $"tweet_id",
                                            $"lang",
                                            $"user_id",
                                            $"user_name",
                                          
                                            when($"geo_latitude".isNull, $"place_latitude")
                                              .otherwise($"geo_latitude")
                                              .as("latitude"),
                                          
                                            when($"geo_longitude".isNull, $"place_longitude")
                                              .otherwise($"geo_longitude")
                                              as("longitude"),
                                          
                                            $"text",
                                            $"time"
                                            ).persist()
  
  
    /*<< INFO >>*/logInfo("Received " + readyTweetsDF.count.toString() + " tweets")
    readyTweetsDF.show()
  
  

    
    
  
  
    /*.............................................*
     * SENTIMENT evaluation
     *.............................................*/
    
    val sentimentDF = elaborateSentiment(readyTweetsDF)
    
    
    
    
    
    
    
    /*.............................................*
     * HASHTAG detection
     *.............................................*/
    val hashtagDF = elaborateHashtags( rawTweetsDF)
  
    
  
  
  
  
  
    /*.............................................*
     * TOPICS detection
     *.............................................*/
  
    val topicsDF = elaborateTopics(rawTweetsDF)
  
  
  
    
  
  
    /*.............................................*
      STORE results to DB
     *.............................................*/
    
    storeDataFrameToCASSANDRA(readyTweetsDF, sentimentDF, hashtagDF, topicsDF)




    
    
  }  //end runElaborator method //
  
  
  
  
  
  /*..................................................................................................................*/
	/**
		*This method evaluates, for each tweet, its average sentiment value with a confidency indicator
    *
		* @param inputTweetsDF a DataFrame with all tweet infos
		* @return a DataFrame with sentiment infos composed by following fields:
    *         "tweet_id", "sentiment_value", "matched_words", "tweet_words", "confidency_value"
		*/
  def elaborateSentiment(inputTweetsDF: DataFrame) : DataFrame = {
  
    
    
    /*
    
    
     */
    
    //getting the hedonometer dictionary (as DataFrame)
    val sentixDF = Sentix.getSentix
    
    
    
    
    /*------------------------------------------------*
     * UDF definitions
     *------------------------------------------------*/
  
    /**
      *
      */
    val confidencyValue = udf( (matched_words: Double, tweet_words: Double) =>{  matched_words/tweet_words  })
  
  
    //sanitization by lower case and regular expression (only dictionary word extracted)
    val sanitizeTweet = udf (( word: String) =>{
      val regularExpression = "\\w+(\'\\w+)?".r
      val sanitizedWord = regularExpression.findFirstIn(word.toLowerCase)
      val emptyWord = ""
      sanitizedWord match
      {
        case None            => emptyWord
        case Some(something) => something     }
    })
  
  
    
    
    
    
    /*---------------------------------------------------*
     * Tweet words sanitization
     *---------------------------------------------------*/
  
    /*
    explode  text(n-words)(1-row) field in
             word(1-word)(n-rows) one
     */
    val explodedTWEETS = inputTweetsDF.select($"tweet_id", $"text")
                                      .explode("text", "word"){text: String => text.split(" ")}
  
  
  
  
  
    //sanitize words by udf
    val sanitizedTWEETS = explodedTWEETS.select($"tweet_id", sanitizeTweet(explodedTWEETS("word")).as("word"))
  
  
  
  
  
  
  
    /*---------------------------------------------------*
     * Sentiment evaluation
     *---------------------------------------------------*/
  
    //count tweets words in a new DataFrame
    val wordCountByTweetDF = countTweetsWords(sanitizedTWEETS)
    
    
    
    
    //joining tweets with Hedonometer dictionary
    val sentimentTWEETS = sentixDF
                                  .join(sanitizedTWEETS, sentixDF("sentix_word") === sanitizedTWEETS("word"), "inner")
                                  .groupBy("tweet_id")
                                  .agg( "absolute_sentiment"  -> "avg",
                                        "word"                -> "count" )
                                  .withColumnRenamed("avg(absolute_sentiment)","sentiment_value")
                                  .withColumnRenamed("count(word)","matched_words")
    
    
    
    //pack results into a new DataFrame
    val sentimentConfidencyTWEETS = sentimentTWEETS
                                                .join( wordCountByTweetDF, "tweet_id")
                                                .select(  $"tweet_id",
                                                          $"sentiment_value",
                                                          $"matched_words",
                                                          $"tweet_words",
                                                          confidencyValue($"matched_words", $"tweet_words").as("confidency_value")
                                                        )

    
    sentimentConfidencyTWEETS
    
  }//end elaborateSentiment method //
  
  
  
  
  
	
	
	
	
  
  /*..................................................................................................................*/
  /**
    * This method counts, for each tweet_id, how many words its text is formed by
    *
    * @param inputDF: DataFrame with following fields: "tweet_id", "word" (already sanitized)
    * @return a DataFrame with two fields: "tweet_id", "tweet_words"
    */
  def countTweetsWords(inputDF: DataFrame): DataFrame = {
    
    

    //count original tweet words
    val wordCountByTweetDF = inputDF.groupBy("tweet_id").count()
                                    .withColumnRenamed("count","tweet_words")
    

    
    wordCountByTweetDF
    
    
  }//end method //
  
  
  
  
  
  
  


  
  
  /*..................................................................................................................*/
	/**
		* This method provides a DataFrame with a full hashtag column, mantaining the correct association with
		* their tweet_id.
		*
		* @param inputDF DataFrame with all tweets info (included hashtagList!)
		* @return DataFrame which associates each tweet_id with its own hashtags
		*
		* @note there could be more than one row per tweet_id! there will be one row for each associated hashtag
		*/
  def elaborateHashtags(inputDF:  DataFrame): DataFrame = {
	  
	  
	  
	  /*---------------------------------------------------*
     * UDF DEFINITION
     *---------------------------------------------------*/
    val identifyNull = udf (( hashtag: String) =>{  if (hashtag.length() > 0) hashtag.toLowerCase()
    else null        }
    )
    
    
    
    
    val tweetsHashtagsDF = inputDF.select($"tweet_id", $"text", $"hashtag_list").explode("hashtag_list", "hashtag") {
      
      //explode  hashtag_list (n-words)(1-row) field in
      //         hashtag     (1-word)(n-rows) one
      hashtagList: String => hashtagList.split(" ")
      
    }
    

    
    
    
    tweetsHashtagsDF.show()
    val filteredHashtagDF = tweetsHashtagsDF.filter(not(isnull(identifyNull(tweetsHashtagsDF("hashtag"))))).select($"tweet_id", $"hashtag")
    filteredHashtagDF.show(30, false)
    
    
    
    
    
    filteredHashtagDF
    
  }// end hashtagExplode  method //
  
  
  
  
  
  
  
  
  
  /*..................................................................................................................*/
  /**
	  * This method provides a DataFrame with a full topic column, mantaining the correct association with
	  * their tweet_id.
	  *
    * @param inputDF a DataFrame with all tweets info (included hashtagList!)
    * @return DataFrame which associates each tweet_id with its own topics
	  *
	  * @note there could be more than one row per tweet_id! there will be one row for each associated topic
    */
  def elaborateTopics(inputDF: DataFrame): DataFrame ={
    
    val topicDF = TopicFinder.findTopic(inputDF)
    
    topicDF
    
  }// end elaborateTopics method //
  

  
	
	
	
  
  
  /*....................................................................................................................*/
  /**
    * Method which stores DataFrames with elaborated values into CASSANDRA tables
    *
    * @param tweetProcessedDF DataFrame containing already processed tweets with final values
    * @param sentimentDF      DataFrame containing tweet sentiment and confidency evaluation
    * @param hashtagDF        DataFrame containing tweet id and related hashtags
    * @param topicsDF          DataFrame containing tweet id and  related topics
    */
  def storeDataFrameToCASSANDRA ( tweetProcessedDF: DataFrame,  sentimentDF: DataFrame, hashtagDF: DataFrame, topicsDF: DataFrame) {
  
  
    tweetProcessedDF.show()
    //myDataStorer.storeTweetsToCASSANDRA(tweetProcessedDF)
    /*<<INFO>>*/  logInfo("tweets storing completed!")
  
    sentimentDF.show()
    //myDataStorer.storeSentimentToCASSANDRA(sentimentDF)
    /*<<INFO>>*/  logInfo("sentiment storing completed!")
  
    hashtagDF.show()
    //myDataStorer.storeHashtagToCASSANDRA(hashtagDF)
    /*<<INFO>>*/  logInfo("hashtag storing completed!")
  
    topicsDF.show()
    //myDataStorer.storeTopicsToCASSANDRA(topicsDF)
    /*<<INFO>>*/  logInfo("topics storing completed!")

    
    
    
  }//end storeDataFrameToCASSANDRA method //
  
  
  
  
  
}//end  TweetApp Class |||||||||||||||||||||||||