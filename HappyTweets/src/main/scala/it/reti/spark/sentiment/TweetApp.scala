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
		* Method TRAIT.
		* MUST BE OVERRIDED with correct input format
		* @param rawTweets: DataFrame with input data from acquireData method
		*/
	def prepareData(rawTweets: DataFrame)
		
		

		
		
		

		

	
	
	
	
  
  
  
  /*.................................................................................................................*/
	/**
		* Method which elaborates tweet DataFrames evaluating sentiment values, hashtags and topics.
		*
		* @param tweetsDF	DataFrame with all tweet potentially useful fields
		* @param hashtagsDF: DataFrame with all hashtags
		* @note hashtagsDF can be null!
		*/
  def runElaborator(tweetsDF: DataFrame, hashtagsDF: DataFrame)  = {
    
    
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
    val readyTweetsDF = tweetsDF.filter(not(isnull($"tweet_id")))
	    
	                              //Family Filter
	                              .filter(not(  $"text".contains("porno")     ||
	                                            $"text".contains("videochat") ||
	                                            $"text".contains("sesso")     ||
	                                            $"text".contains("puttana")   ||
	                                            $"text".contains("troia")         ))
	    
	                              .select(
													                $"tweet_id",
													                $"lang",
													                $"user_id",
													                $"user_name",
		                                      $"screen_name",
		                                      $"retweets_something",
		                              
													              
													                when($"geo_latitude".isNull, $"place_latitude")
													                  .otherwise($"geo_latitude")
													                  .as("latitude"),
													              
													                when($"geo_longitude".isNull, $"place_longitude")
													                  .otherwise($"geo_longitude")
													                  as("longitude"),
													              
													                $"text",
													                $"time",
													                $"region"
													                )//end select
	
	
	  
	  
	
	  //>>>>>>>>>> MEMORY PERSIST >>>>>>>>> |##|##|##|
	  readyTweetsDF.persist()
    hashtagsDF.persist()
	  //>>>>>>>>>> MEMORY PERSIST >>>>>>>>> |##|##|##|
	  
	  
	  
  
    

  
  /*.............................................*
   * SENTIMENT evaluation
   *.............................................*/

	  val sentimentDF = elaborateSentiment(readyTweetsDF)



  
  /*.............................................*
   * TOPICS detection
   *.............................................*/
	  
	  val topicsDF = elaborateTopics(hashtagsDF, readyTweetsDF)
	
	
	  
	  
	  //>>>>>>>>>> MEMORY PERSIST >>>>>>>>> |##|##|##|
	  topicsDF.persist()
	  sentimentDF.persist()
	  //>>>>>>>>>> MEMORY PERSIST >>>>>>>>> |##|##|##|
	  
	  
	  
	  
  
  /*.............................................*
    STORE results to DB
   *.............................................*/
      
	  storeDataFrameToCASSANDRA(readyTweetsDF, sentimentDF, hashtagsDF, topicsDF)


	  
	  

		
	  //<<<<<<<<<<<< MEMORY FREE <<<<<<<<<<<<<<<  |  |  |  |  |
	  readyTweetsDF.unpersist()
	  hashtagsDF.unpersist()
	  topicsDF.unpersist()
	  sentimentDF.unpersist()
	  //<<<<<<<<<<<< MEMORY FREE <<<<<<<<<<<<<<<  |  |  |  |  |

	  
    
    
  }  //end runElaborator method //
  
  
	
  
  
  
  /*..................................................................................................................*/
	/**
		*This method evaluates, for each tweet, its average sentiment value with a confidency indicator
    *
		* @param inputTweetsDF a DataFrame with all tweet infos
		* @return a DataFrame with sentiment infos composed by following fields:
    *         "tweet_id", "sentiment_value", "matched_words", "tweet_words", "confidency_value"
		*/
  private def elaborateSentiment(inputTweetsDF: DataFrame) : DataFrame = {
	  
    
    //get the hedonometer dictionary (as DataFrame)
    val sentixDF = Sentix.getSentix
    
    
    
    
    /*------------------------------------------------*
     * UDF definitions
     *------------------------------------------------*/
  
	  
	  
    /** UDF function to evaluate the confidency value as (matched_words / total_tweet_words) */
    val confidencyValue = udf( (matched_words: Double, tweet_words: Double) =>{  matched_words/tweet_words  })
  
  
	  
	  
	  
	  /** UDF function to extract a clean word from a generic already-split string*/
    val sanitize_english_tweets = udf (( word: String) =>{
	    
				      val regularExpression = "\\w+(\'\\w+)?".r
	    
	    
	    
				      val sanitizedWord = regularExpression.findFirstIn(word.toLowerCase)
				      
	    sanitizedWord match
				      {
				        case None            => null
				        case Some(something) => something     }
    
    })// end UDF sanitize_english_tweets function //
	
	
	  
	  
	
	
	  /** UDF function to extract a clean word from a generic already-split string*/
	  val sanitize_italian_tweets = udf (( word: String) =>{
		
		  val regularExpression = "\\w+".r
		  val sanitizedWord = regularExpression.findAllIn(word.toLowerCase).toList
		  
		  if (sanitizedWord.nonEmpty)
			  sanitizedWord.last
		  else null
		  
		
	  })// end UDF sanitize_italian_tweets function //
	  
	  
	  
	  
	  
	  
	  
  

    
    /*---------------------------------------------------*
     * Tweet words sanitization
     *---------------------------------------------------*/
	
	  
	  
    /*
    explode  text(n-words)(1-row) field in
             word(1-word)(n-rows) one
     */
    val explodedTWEETS = inputTweetsDF.select($"tweet_id", $"lang", $"text")
                                      .explode("text", "word"){
	                                                              text: String => text.split(" ")
                                                                                                    }
	

  
  
  
    //sanitize words by udf
    val sanitizedTWEETS = explodedTWEETS.select(
																							    $"tweet_id",
																							    when($"lang".equalTo("en"), sanitize_english_tweets(explodedTWEETS("word")))
																								    .otherwise(sanitize_italian_tweets(explodedTWEETS("word")))
																								    .as("word")
																						    )//end select
																						    .filter(not(isnull($"word")))
	
	  
  

  
  
  
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
  private def countTweetsWords(inputDF: DataFrame): DataFrame = {
	  

    //count original tweet words
    inputDF.groupBy("tweet_id")
	          .agg( "word"-> "count" )
	          .withColumnRenamed("count(word)","tweet_words")
    
    
  }//end method //
  
  
  
  
  
  

  
  
  
  
  /*..................................................................................................................*/
  /**
	  * This method provides a DataFrame with a full topic column, mantaining the correct association with
	  * their tweet_id.
	  *
    * @param hashtagDF a DataFrame with "tweet_id", "text" and "hashtag" infos
	  * @param tweetsDF a DataFrame with all tweets infos
    * @return DataFrame which associates each tweet_id with its own topics
	  *
	  * @note there could be more than one row per tweet_id! there will be one row for each associated topic
    */
  private def elaborateTopics(hashtagDF: DataFrame, tweetsDF: DataFrame): DataFrame ={
    
    /*
    Check if hashtagDF is not null.
    If so, find topic with full infos accessible.
    Otherwise a new DataFrame with "tweet_id", "text" and "hashtag= null" is passed.
      */
	  
	  if(hashtagDF.count() > 0) TopicFinder.findTopic_hashtag_and_text(hashtagDF)
	  else

		  TopicFinder.findTopic_text_only(
			
			  tweetsDF.select(
				  $"tweet_id",
				  $"text",
				  lit(null: String).as("hashtag")
			  )
		  )
	  
    
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
  private def storeDataFrameToCASSANDRA ( tweetProcessedDF: DataFrame,  sentimentDF: DataFrame, hashtagDF: DataFrame, topicsDF: DataFrame) {
	
	
	
	  /*<<INFO>>*/logInfo("Elaborating tweets...")/*<<INFO>>*/
	  tweetProcessedDF.show()
	  /*<< INFO >>*/ logInfo("Received " + tweetProcessedDF.count.toString() + " tweets") /*<< INFO >>*/
	  /*<<INFO>>*/logInfo("Tweets elaborated! >>>>  Now saving to Cassandra... ")/*<<INFO>>*/
	  myDataStorer.storeTweetsToCASSANDRA(tweetProcessedDF)
    /*<<INFO>>*/  logInfo("Tweets storing completed!") /*<<INFO>>*/
	
	  
	
	  /*<<INFO>>*/logInfo("Elaborating sentiment...")/*<<INFO>>*/
	  sentimentDF.show()
	  /*<<INFO>>*/logInfo("Sentiment elaborated! >>>>  Now saving to Cassandra...")/*<<INFO>>*/
	  myDataStorer.storeSentimentToCASSANDRA(sentimentDF)
    /*<<INFO>>*/  logInfo("Sentiment storing completed!")/*<<INFO>>*/
	
	  
	
	  /*<<INFO>>*/logInfo("Elaborating hashtags...")/*<<INFO>>*/
	  hashtagDF.show()
	  /*<< INFO >>*/ logInfo("Found "    + hashtagDF.count.toString()    + " hashtags") /*<< INFO >>*/
	  /*<<INFO>>*/logInfo("Hashtag elaborated! >>>>  Now saving to Cassandra... ")/*<<INFO>>*/
	  myDataStorer.storeHashtagToCASSANDRA(hashtagDF.select($"tweet_id", $"hashtag"))
    /*<<INFO>>*/  logInfo("Hashtag storing completed!") /*<<INFO>>*/
	
	
	  
	  /*<<INFO>>*/logInfo("Evaluating topics...")/*<<INFO>>*/
	  topicsDF.show()
	  /*<<INFO>>*/logInfo("Topics evaluated! >>>>  Now saving to Cassandra...")/*<<INFO>>*/
	  myDataStorer.storeTopicsToCASSANDRA(topicsDF)
    /*<<INFO>>*/  logInfo("Topics storing completed!") /*<<INFO>>*/
	
	
	
	
  }//end storeDataFrameToCASSANDRA method //
	
	
	
	
	
	//------------------------------------------------ UNUSED METHOD!!!
	/*
		
		
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
		
		
		
		*/
	
	
	
	
}//end  TweetApp Class |||||||||||||||||||||||||