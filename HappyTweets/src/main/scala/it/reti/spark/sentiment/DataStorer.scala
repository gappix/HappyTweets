package it.reti.spark.sentiment

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.cassandra





/*°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°*/
/**
 * This object stores processing DataFrames into correct CASSANDRA tables
 */

class DataStorer(processingType: String) extends Serializable with Logging{
  
   
  
  //get sqlHIVE context and import methods for DataFrame/RDD conversion 
  private val sqlContextHIVE = ContextHandler.getSqlContextHIVE
  import sqlContextHIVE.implicits._
  

  /*
   * Keyspace and tablese arer already created in Cassandra DB using its own chsql shell
   */
  
  //CASSANDRA tables
  val tableTweets     = "event_processed_" + processingType
  val tableSentiment  = "event_sentiment_" + processingType
  val tableHashtag    = "event_hashtag_"   + processingType
  val tableTopics     = "event_topic_"    + processingType
 
  //CASSANDRA keyspace
  val keyspaceCassandra = "qlik"
  
  
  
  
  //.................................................................................................................
  /**
   * method to store tweet infos into CASSANDRA tableTweets
   * @param tweetDF: a DataFrame of elaborated tweets ready to be stored
   */
  def storeTweetsToCASSANDRA (tweetDF: DataFrame) = {
        
        
        tweetDF.write.format("org.apache.spark.sql.cassandra").option("table",tableTweets).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()
        
    
  }//end storeTweetsToCASSANDRA method //
  
  
 
  
  //.................................................................................................................
  /**
   * method to store sentiment infos into CASSANDRA tableSentiment
   * @param sentimentDF: a DataFrame of elaborated sentiment values ready to be stored
   */
  def storeSentimentToCASSANDRA (sentimentDF: DataFrame) = {
    
    
        sentimentDF.write.format("org.apache.spark.sql.cassandra").option("table",tableSentiment).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()

        
  }//end storeSentimentToCASSANDRA method //
  

  
  
  
  //.................................................................................................................
  /**
   * method to store hashtag infos into CASSANDRA tableSentiment
   * @param hashtagDF: a DataFrame of elaborated hashtag values ready to be stored
   */
  def storeHashtagToCASSANDRA (hashtagDF: DataFrame) = {
        
    
        hashtagDF.write.format("org.apache.spark.sql.cassandra").option("table",tableHashtag).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()     


  }//end storeHashtagToCASSANDRA method //
  
  
  
  
  
  //.................................................................................................................
  /**
    * method to store topic infos into CASSANDRA tableSentiment
    * @param topicsDF: a DataFrame of elaborated topic values ready to be stored
    */
  def storeTopicsToCASSANDRA (topicsDF: DataFrame) = {
    
    
        topicsDF.write.format("org.apache.spark.sql.cassandra").option("table",tableTopics).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()        
    
    
  }//end storeTopicsToCASSANDRA method //
  
  
  
  
  
}//end DataStorer class //