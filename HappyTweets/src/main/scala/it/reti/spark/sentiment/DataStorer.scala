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
  val tableTweets     = "test2_processed_" + processingType
  val tableSentiment  = "test2_sentiment_" + processingType
  val tableHashtag    = "test2_hashtag_"   + processingType
  val tableTopics     = "test2_topic_"    + processingType
 
  //CASSANDRA keyspace
  val keyspaceCassandra = "qlik"
  
  
  
  
  //.................................................................................................................
  /**
   * method to store tweet infos into CASSANDRA tableTweets
   * @param tweetDF: a DataFrame of elaborated tweets ready to be stored
   */
  def storeTweetsToCASSANDRA (tweetDF: DataFrame) = {
        
        /*<<INFO>>*/  logInfo("Writing tweets into CASSANDRA table...")
        tweetDF.write.format("org.apache.spark.sql.cassandra").option("table",tableTweets).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()
        /*<<INFO>>*/  logInfo("The following content has successfully been stored:")       
        
        

    
  }//end storeTweetsToCASSANDRA method //
  
  
 
  
  //.................................................................................................................
  /**
   * method to store sentiment infos into CASSANDRA tableSentiment
   * @param sentimentDF: a DataFrame of elaborated sentiment values ready to be stored
   */
  def storeSentimentToCASSANDRA (sentimentDF: DataFrame) = {
        
        /*<<INFO>>*/ logInfo("Writing sentiment results into CASSANDRA table...") 
        sentimentDF.write.format("org.apache.spark.sql.cassandra").option("table",tableSentiment).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()
        /*<<INFO>>*/  logInfo("The following content has successfully been stored:")  
   


  }//end storeSentimentToCASSANDRA method //
  

  
  
  
  //.................................................................................................................
  /**
   * method to store hashtag infos into CASSANDRA tableSentiment
   * @param hashtagDF: a DataFrame of elaborated hashtag values ready to be stored
   */
  def storeHashtagToCASSANDRA (hashtagDF: DataFrame) = {
        
        /*<<INFO>>*/ logInfo("Writing sentiment results into CASSANDRA table...") 
        hashtagDF.write.format("org.apache.spark.sql.cassandra").option("table",tableHashtag).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()
        /*<<INFO>>*/  logInfo("The following content has successfully been stored:")  
        


  }//end storeHashtagToCASSANDRA method //
  
  
  
  
  
  //.................................................................................................................
  /**
    * method to store topic infos into CASSANDRA tableSentiment
    * @param topicsDF: a DataFrame of elaborated topic values ready to be stored
    */
  def storeTopicsToCASSANDRA (topicsDF: DataFrame) = {
    
        /*<<INFO>>*/ logInfo("Writing sentiment results into CASSANDRA table...")
        topicsDF.write.format("org.apache.spark.sql.cassandra").option("table",tableTopics).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()
        /*<<INFO>>*/  logInfo("The following content has successfully been stored:")
       
    
    
  }//end storeTopicsToCASSANDRA method //
  
  
  
  
  
}//end DataStorer class //