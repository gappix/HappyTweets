package horizon

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql._

import scala.util.Try







/*||||||||||||||||||||||||||||||||||||||||||||  TWEET STREAMING APP   ||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
  * This class is an extension of TweetJSONApp one.
  * It implements a specific acquireData method for streaming data retrieving from a twitter stream coming from a socket.
  * It then uses upper-class methods for data elaboration and result storing.
  *
  * @param locationToObserve: String containing an integer number according to Location class options
  * @param batchInterval: Integer value which indicates how many seconds each batch should last
 */
 class TweetStreamingJSONApp (locationToObserve : String, batchInterval: Int) extends TweetJSONApp("streaming") {



  @transient lazy override val log = org.apache.log4j.LogManager.getLogger("horizon")





  /*.................................................................................................................*/
  /**
   * Run method OVERRIDED in order to fulfill streaming app processing needings
   * It takes spout source data and packs everything into a single DataFrame every 25 seconds.
   * Each DataFrame structure is then passed to upper-class "elaborate" method in order to retrieve sentiment evaluation.
   * Results are eventually stored into HIVE tables by invoking upper-class "storeDataFrameToHIVE" method.
   */
	 def acquireData() {
  
  
  
    //interval settings
    val intervalSeconds = batchInterval /* <---- set this to best interval for your operation */
  
    //port for socket listener
    val portToListen = 9999
    
    
    
    //spark streaming context created
    val conf= ContextHandler.getConf
    val ssc = new StreamingContext(conf, Seconds(intervalSeconds))

    
    
    
    

    
    
    
    
    
    /*------------------------------------------------
     * Stream input and transformations
     *-----------------------------------------------*/
    
    
    /*<<< INFO >>>*/ log.info("Opening Twitter stream...") /*<<< INFO >>>*/
    
    val myReceiver = new SocketReceiver(portToListen)
    val tweetsStream = ssc.receiverStream(myReceiver)
    
    /*<<< INFO >>>*/ log.info("Stream opened!") /*<<< INFO >>>*/
  
  
  
  

    
    
   /*----------------------------------------------------
 	  * Transformations for each RDD
   	*---------------------------------------------------*/     
    tweetsStream.foreachRDD { rdd    =>


			/*<<< INFO >>>*/ log.info("building context")
			/*<<< INFO >>>*/ log.info(rdd.sparkContext.getConf.toString)

      //SparkSession created
      val spark = SparkSession.builder
															.config(rdd.sparkContext.getConf)
															.getOrCreate()
      import spark.implicits._




			/*<<< INFO >>>*/ log.info(rdd.toString() +  " started!")
			/*<<< INFO >>>*/ log.info("reading json input...")
			val dataDF  = spark.read.json(rdd).persist()




			log.info(spark.toString)

			/*....................................................
			 check if there is any message to process;
			 if true run elaborator methods of the parent class
			*......................................................*/
			if(Try(dataDF("id")).isSuccess)    prepareData(dataDF)

			dataDF.unpersist()



      /*<<< INFO >>>*/ log.info(rdd.toString() +  " Processing completed!") /*<<< INFO >>>*/
      /*<<< INFO >>>*/ log.info("\n\n ========================================== END ROUND ============================================>>>\n\n\n") /*<<< INFO >>>*/
    


    }//end foreachRDD
 
    
  
    

    
    
    
   /*------------------------------------------------
    * Streaming start, await and stop
    *-----------------------------------------------*/

    ssc.start()
    ssc.awaitTermination()
    
    
    
    
  }//end Run (overrided) method //
  
  
  
  
  

  


  
  
  
   
}//end TweetStreamingApp class |||||||||||||

