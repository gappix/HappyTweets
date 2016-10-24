package it.reti.spark.functional

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf




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
object Sentix {


	/**
		*
		* @param inputTextDictionary
		* @return
		*/
	def build_dictionary(inputTextDictionary: Dataset[String]): DataFrame = {



		import inputTextDictionary.sparkSession.implicits._


		//DataFrame creation
		val sentixDS = inputTextDictionary
												.map(_.split("\t")) //split on tab spaces
												.map(//RDD association with Sent case class
																row => Sent(row(0), row(1), row(2).toInt, row(3).toFloat, row(4).toFloat, row(5).toFloat, row(6).toFloat)
															)

												.toDF     //DataFrame creation
												.as[Sent] //Dataset (typed) creation











                               
     /***/
     val evaluate_AbsoluteSentiment = udf ((p: Float, n: Float) =>{	Math.sqrt(0.25*(p*p) + 0.25*(n*n) + 0.25 - 0.5*n + 0.5*p -0.5*n*p)   })
                                                                                                                   
       
  /*
  
  
   */
    val sentix_absoluteDF = sentixDS
	   .where(not($"sentix_word".contains("_")))
     .select(
               lower($"sentix_word").as("sentix_word"),
               evaluate_AbsoluteSentiment(sentixDS("positive_score"), sentixDS("negative_score")).as("absolute_sentiment"),
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





		sentix_absoluteDF



	}//end build_dictionary method //
	


    
    
}// end  Sentix object |||||||||||
