package it.reti.spark.functional

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


//|||||||||||||||||||||||||||||||||||||||||||||||    MAIN    |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
/**
 *  Main object for the SPARK Twitter Analyzer app
 */  
object Main{




  /*.................................................................................................................*/
  /**
    * main method definition
    * @param args "batch" or "streaming + #batch-seconds" string to select desired execution mode
    */
  def main(args: Array[String])  {


    //app logger global settings
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

		val rawTweetsDF = spark.read.json(AppSettings.input_file).persist()

		rawTweetsDF







  
  
}//end main object |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||