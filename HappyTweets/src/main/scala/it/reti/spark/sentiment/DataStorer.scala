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
  val tableTweets     = "test_processed_" + processingType
  val tableSentiment  = "test_sentiment_" + processingType
  val tableHashtag    = "test_hashtag_"   + processingType
  val tableTopics     = "test_topics_"    + processingType
 
  //CASSANDRA keyspace
  val keyspaceCassandra = "qlik"
  
  
  
  
  //.................................................................................................................
  /**
   * method to store tweet infos into CASSANDRA tableTweets
   * @param tweetDF: a DataFrame of elaborated tweets ready to be stored
   */
  def storeTweetsToCASSANDRA (tweetDF: DataFrame) = {
        
        /*<<INFO>>*/  logInfo("Writing tweets into CASSANDRA table...")
        tweetDF.persist().write.format("org.apache.spark.sql.cassandra").option("table",tableTweets).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()
        /*<<INFO>>*/  logInfo("The following content has successfully been stored:")       
        tweetDF.show()
        tweetDF.unpersist()
        

    
  }//end storeTweetsToCASSANDRA method //
  
  
 
  
  //.................................................................................................................
  /**
   * method to store sentiment infos into CASSANDRA tableSentiment
   * @param sentimentDF: a DataFrame of elaborated sentiment values ready to be stored
   */
  def storeSentimentToCASSANDRA (sentimentDF: DataFrame) = {
        
        /*<<INFO>>*/ logInfo("Writing sentiment results into CASSANDRA table...") 
        sentimentDF.persist().write.format("org.apache.spark.sql.cassandra").option("table",tableSentiment).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()
        /*<<INFO>>*/  logInfo("The following content has successfully been stored:")  
        sentimentDF.show()
        sentimentDF.unpersist()


  }//end storeSentimentToCASSANDRA method //
  

  
  
  
  //.................................................................................................................
  /**
   * method to store hashtag infos into CASSANDRA tableSentiment
   * @param hashtagDF: a DataFrame of elaborated hashtag values ready to be stored
   */
  def storeHashtagToCASSANDRA (hashtagDF: DataFrame) = {
        
        /*<<INFO>>*/ logInfo("Writing sentiment results into CASSANDRA table...") 
        hashtagDF.persist().write.format("org.apache.spark.sql.cassandra").option("table",tableHashtag).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()
        /*<<INFO>>*/  logInfo("The following content has successfully been stored:")  
        hashtagDF.show()
        hashtagDF.unpersist()


  }//end storeHashtagToCASSANDRA method //
  
  
  
  
  
  //.................................................................................................................
  /**
    * method to store topic infos into CASSANDRA tableSentiment
    * @param topicsDF: a DataFrame of elaborated topic values ready to be stored
    */
  def storeTopicsToCASSANDRA (topicsDF: DataFrame) = {
    
        /*<<INFO>>*/ logInfo("Writing sentiment results into CASSANDRA table...")
        topicsDF.persist().write.format("org.apache.spark.sql.cassandra").option("table",tableTopics).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()
        /*<<INFO>>*/  logInfo("The following content has successfully been stored:")
        topicsDF.show()
        topicsDF.unpersist()
    
    
  }//end storeTopicsToCASSANDRA method //
  
  
  
  
  
}//end DataStorer class //