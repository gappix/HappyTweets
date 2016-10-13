package it.reti.spark.sentiment

import org.apache.spark.sql._


/* ||||||||||||||||||||||||||||||||||||||||| TWEET BATCH APP |||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
 * This class is an extension of TweetApp one.
 * It implements a specific Run method for batch data extraction from a file database.
 * It then uses upper-class methods for data elaboration and result storing.  
 * 
 * @param fileNameAndPath: path to retrieve input json-dataFile
 */
  
class TweetBatchApp(fileNameAndPath : String) extends TweetJSONApp("batch") {
  
  
  
  
  //.........................................................................................................
  /**
   * Run method OVERRIDED in order to fulfill batch app processing needing
   * It takes source filename (a JSON-row database with tweets data), extracts interesting infos packing 
   * everything in a single DataFrame.
   * This structure is then passed to upper-class "elaborate" method in order to retrieve sentiment evaluation.
   * Results are eventually stored into HIVE tables by invoking upper-class "storeDataFrameToHIVE" method.
   */
  override def acquireData() {
    
    
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    
    
    
    /*--------------------------------------------------------------
     * Input Data preparation
     *--------------------------------------------------------------*/
    
    
    // Tweet json storage load into a DataFrame
    val inputTWEETS = spark.read.json(fileNameAndPath)
   
    
    // Filtering based on language field
    val englishTWEETS = inputTWEETS.filter($"lang".equalTo("en"))
    

    
    
   //DataFrame is created by selecting interested fields from input DataFrame
    prepareData(englishTWEETS, spark)
    
    

  }// end Run method //



}// end TweetBatchApp class ||||||||||||||||