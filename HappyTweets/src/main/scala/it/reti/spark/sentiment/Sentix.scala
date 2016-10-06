package it.reti.spark.sentiment

import scala.reflect.runtime.universe
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import java.lang.Math.sqrt



//case class for Hedonometer structure (needed for DataFrame creation)
case class Sent(
    sentix_word: String,
    wordType: String,
    wordnetID: Int,
    positive_score: Float,
    negative_score: Float,
    polarity: Float,
    intensity: Float
    )
    
    
    
/*|||||||||||||||||||||||||||||||||||||||||||||||||   SENTIX   |||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
 * This class loads the Sentix dictionary from a HDFS text file and structures it as DataFrame
 * It can be accessed by other classes by a getHedonometer invocation   
 */    
object Sentix extends Serializable with Logging{
  
  
  
    //getting Contexts
    val sc = ContextHandler.getSparkContext
    val sqlContextHIVE = ContextHandler.getSqlContextHIVE
    //import methods for DataFrame/RDD conversion
    import sqlContextHIVE.implicits._
    
    
    
    
    //load textfile RDD
    private val inputSENTIX = sc.textFile("/home/administrator/BigData/HappyQlik/Sentix.txt")
	
	// HIVE ---> /user/maria_dev/Tutorials/SPARKTwitterAnalyzer/Sentix.txt
	// CASSY --> /home/administrator/BigData/HappyQlik/Sentix.txt
    
    
    
    //DataFrame creation
    private val sentixDF = inputSENTIX 
                               .map(_.split("\t")) //split on tab spaces 
                               .map( //RDD association with Hedo case class
                                     row => Sent( row(0), row(1), row(2).toInt, row(3).toFloat, row(4).toFloat, row(5).toFloat, row(6).toFloat )
                                     ) 
                               .toDF //DataFrame creation
    
                               
                               
     /***/
     private val evaluate_AbsoluteSentiment = udf ((p: Float, n: Float) =>{   Math.sqrt(0.25*(p*p) + 0.25*(n*n) + 0.25 - 0.5*n + 0.5*p -0.5*n*p)   })
                                                                                                                   
       
  /*
  
  
   */
   private val sentix_absoluteDF = sentixDF
	   .where(not($"sentix_word".contains("_")))
     .select(
               lower($"sentix_word").as("sentix_word"),
               evaluate_AbsoluteSentiment(sentixDF("positive_score"), sentixDF("negative_score")).as("absolute_sentiment"),
               $"positive_score",
               $"negative_score"
              )//end select
     .groupBy($"sentix_word")
     .agg(
            "absolute_sentiment"  -> "avg",
            "positive_score"      -> "avg",
            "negative_score"      -> "avg")
     .withColumnRenamed("avg(absolute_sentiment)","absolute_sentiment")
     .withColumnRenamed("avg(positive_score)",    "positive_score"    )
     .withColumnRenamed("avg(negative_score)",    "negative_score"    )

  
  
     
   
   sentix_absoluteDF.show()
    
    //BROADCAST the data loaded to all cluster nodes
    private val broadCastedSentix = sc.broadcast(sentix_absoluteDF)
     
    
	
	
	

    /*................................................................................................................*/
    /**     
     *  Method to 
     *  @return the entire Sentix dictionary DataFrame from local broadcasted variable
     */
    def getSentix  = broadCastedSentix.value
      


    
    
}// end  Sentix class |||||||||||

