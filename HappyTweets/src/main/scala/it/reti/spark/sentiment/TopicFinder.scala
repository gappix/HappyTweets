package it.reti.spark.sentiment

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
  *
  *
  * Created by gappix on 19/09/2016.
  */
object TopicFinder extends Logging{
  
  
  
  //get sqlHIVE context and import methods for DataFrame/RDD conversion
  val sqlContextHIVE = ContextHandler.getSqlContextHIVE
  import sqlContextHIVE.implicits._
  
  
  /*
  Here is needed a topic definition by setting, for each topic:
          
          1)topic name
          2)a List of keywords referred to this topic

          
   */
  private val topics = new Array[Topic](4)
  
              topics(0) = Topic( "business intelligence",
                                  Seq("businessintelligence", "business intelligence", "qlik", "qlikview", "qlik view", "qliksense", "qlik sense", "data analytics", "dataanalytics", "datascience" ) 
                                  )
  
              topics(1) = Topic( "big data",
                                  Seq("bigdata", "big data", "apachespark", "apache spark", "hadoop", "iot", "datalake", "data lake", "machinelearning")  
                                  )
  
              topics(2) = Topic( "viz your world",
                                  Seq("vizyourworld", "visualize your world")  
                                  )
  
              topics(3) =  Topic( "sentiment analysis",
                                  Seq("sentimentanalysis", "sentiment analysis", "tweetsentiment", "tweet sentiment", "tweets sentiment", "socialmediaanalytics", "social media analytics")
                                  )
  
  
  
  
  
  /*---------------------------------
   * UDF FUNCTIONS DEFINITION
   ----------------------------------*/
  /**
    * this UDF searches for each tweet if it could be labelled with one or more of predefined topics.
    *
    * @return topicsFound: a String list of matching topics separated by one blank space " "
    */
   private val findTopics = udf ((hashtags: String, text: String)  =>{
                
            
                    
                  /*
                  We define a String containing the sequence of topics related to each tweet.
                  Topic words are separated by a blank space " " for further word split and recognition.
                   */
                  var topicsFound = ""
                  
                  
                  
                  for (topic <- topics){
                    
                    //if a topic is found, topic's name is added to topics String list
                    if (topicIsFound(topic, hashtags, text))     topicsFound +=  " " + topic.name
                    
                  }
                  
                  
                 //check if any of preselected topics has been found; otherwise a generic topic "other" is labelled
                  if (topicsFound.length > 0)  topicsFound
                  else "other"
     
     
              }
              )// end findTopics UDF definition //
  
  
  
  /**
    * This UDF method identifies if a DataFrame topic field contains an empty value
    * if true explicits it with a null value, otherwise it gives back original value with a LowerCase sanitization
    */
  val identifyNull = udf (( topic: String) =>{  if (topic.length() > 0) topic.toLowerCase()
                                                else null
                                              }
                                              )// end identifyNull
  
  
  
  

  
  
  
  
  
  
  
  
  /*..................................................................................................................*/
  /**
    *
    * @param tweet a DataFrame with "tweets_id", "text", "hashtag" infos
    * @return a new DataFrame with following fields:  tweet_id  | topic
    *
    * @note there could be multiple lines with the same tweet_id when it is associated to more than one topic
    */
  def findTopic_hashtag_and_text(tweet: DataFrame): DataFrame = {
  
  
    //selecting desired fields and invoking findTopic UDF helper methodr
  
    val groupedHashtagsDF = tweet.groupBy("tweet_id", "text")
      .agg(concat_ws(", ", collect_list("hashtag"))
        .as("hashtag_list"))
  
    
    findTopic_general(groupedHashtagsDF)
  
  
  }//end findTopic_hashtag_and_text method //
  
  
  
  
  
  /*..................................................................................................................*/
  /**
    *
    * @param tweet a DataFrame with only "tweet_id" and "text" infos
    * @return a new DataFrame with following fields:  tweet_id  | topic
    *
    * @note there could be multiple lines with the same tweet_id when it is associated to more than one topic
    */
  def findTopic_text_only(tweet: DataFrame): DataFrame = {
    
    
    //selecting desired fields and invoking findTopic UDF helper methodr
    
    val selectedFieldsDF = tweet.select( $"tweet_id",
                                          $"text",
                                          lit("").as("hashtag_list")
                                        )//end select
    findTopic_general(selectedFieldsDF)
    
  }//end findTopic_hashtag_and_text method //
  
  
  
  
  
  
  
  /*..................................................................................................................*/
  /**
    *
    * @param inputTweet a DataFrame with "tweet_id", "text", "hashtag_list" infos
    * @return a new DataFrame with following fields:  tweet_id  | topic
    *
    * @note there could be multiple lines with the same tweet_id when it is associated to more than one topic
    */
  private def findTopic_general(inputTweet: DataFrame): DataFrame = {
    

  
    
    val rawTopicsDF = inputTweet.select($"tweet_id", findTopics($"hashtag_list", $"text").as("topic_list"))

    /*
    explode  topicList (n-words)*(1-row) field in
             topic     (1-words)*(n-rows) one
     */
    val tweetsTopicsDF = rawTopicsDF.explode("topic_list", "topic") {
                                                                      topicList: String => topicList.split(" ")
                                                                    }
    
    val filteredTopicsDF = tweetsTopicsDF.filter(not(isnull(identifyNull(tweetsTopicsDF("topic")))))
                                          .select($"tweet_id", $"topic")

    
    filteredTopicsDF
    

    
  }//end findTopic_general method //
  
  
  
  
  
  
  
  
  
  
  
  
  /*..................................................................................................................*/
  /**
    * Method which check if tweet content matches a specific topic
    *
    * @param topicSearched: topic (of Topic type) to check
    * @param hashtags: tweets hashtags
    * @param text: tweet text
    * @return true if a topic matching is found, false otherwise
    */
  private def topicIsFound(topicSearched:Topic, hashtags:String, text:String): Boolean = {
    
    
    /*
    
    
     */
    
    
    //hashtag search
    for (key_word <- topicSearched.key_words)
      if(hashtags.contains(key_word) || text.toLowerCase.contains(key_word) ) return true

    false

  }// end topicIsFound method //
  
  
  
  
  
  
  
  
}// end TopicFinder class |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||