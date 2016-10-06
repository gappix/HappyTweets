package it.reti.spark.sentiment

import scala.reflect.runtime.universe

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._ 
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging

import scala.util.Try


/*||||||||||||||||||||||||||||||||||||||||||||||   HASHTAG TWEETS   ||||||||||||||||||||||||||||||||||||||||||||||||||*/
case class HashtagTweets( tweet_id: Long, hashtagList: String, text: String)









/*||||||||||||||||||||||||||||||||||||||||||||  TWEET STREAMING APP   ||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
 * This class is an extension of TweetApp one.
 * It implements a specific Run method for streaming data retrieving from a twitter stream spout.
 * It then uses upper-class methods for data elaboration and result storing.  
 * 
 * @param locationToObserve: String containing an integer number according to Location class options
 */
class TweetStreamingJSONApp(locationToObserve : String, batchInterval: Int) extends TweetJSONApp("streaming") with Logging {
  
  

  
  
  
  /*.................................................................................................................*/
  /**
   * Run method OVERRIDED in order to fulfill streaming app processing needings
   * It takes spout source data and packs everything into a single DataFrame every 25 seconds.
   * Each DataFrame structure is then passed to upper-class "elaborate" method in order to retrieve sentiment evaluation.
   * Results are eventually stored into HIVE tables by invoking upper-class "storeDataFrameToHIVE" method.
   */
  override def acquireData() {
  
  
  
    //interval settings
    val intervalSeconds = batchInterval /* <---- set this to best interval for your operation */
  
    //port for socket listener
    val portToListen = 9999
    
    
    
    //import context needed
    val sc = ContextHandler.getSparkContext
    val ssc = new StreamingContext(sc, Seconds(intervalSeconds))
    val sqlContext = ContextHandler.getSqlContextHIVE
    import sqlContextHIVE.implicits._
    
    
    
    
    

    
    
    
    
    
    /*------------------------------------------------
     * Stream input and transformations
     *-----------------------------------------------*/
    
    
    /*<<< INFO >>>*/ logInfo("Opening Twitter stream...") /*<<< INFO >>>*/
    
    val myReceiver = new SocketReceiver(portToListen)
    val tweetsStream = ssc.receiverStream(myReceiver)
    
    /*<<< INFO >>>*/ logInfo("Stream opened!") /*<<< INFO >>>*/
  
  
  
  

    
    
   /*----------------------------------------------------
 	  * Transformations for each RDD
   	*---------------------------------------------------*/     
    tweetsStream.foreachRDD { rdd    =>
  
      
            /*<<< INFO >>>*/ logInfo(rdd.toString() +  " started!")
            val dataDF  = sqlContext.read.json(rdd).persist()


            /*....................................................
             check if there is any message to process;
             if true run elaborator methods of the parent class
            *......................................................*/
              
            // if(dataDF.persist().count() > 0)    
            if(Try(dataDF("id")).isSuccess)    prepareData(dataDF)
                  
 
            dataDF.unpersist()
  
      /*<<< INFO >>>*/ logInfo(rdd.toString() +  " Processing completed!") /*<<< INFO >>>*/
      /*<<< INFO >>>*/ logInfo("\n\n ========================================== END ROUND ============================================>>>\n\n\n") /*<<< INFO >>>*/
    


    }//end foreachRDD
 
    
  
    

    
    
    
   /*------------------------------------------------
    * Streaming start, await and stop
    *-----------------------------------------------*/

    ssc.start()
    ssc.awaitTermination()
    
    
    
    
  }//end Run (overrided) method //
  
  
  
  
  
  
  /*.................................................................................................................*/
  /**
   * Method that
   * @return (GeoLocation latitude, GeoLocation longitude) if present, (None None) otherwise
   */
  def getGeoLocationCoordinates( status : twitter4j.Status) : (Option[Double], Option[Double]) = {
     
    
    status.getGeoLocation match{
      
      case null => (None, None)
      case default => (Some(status.getGeoLocation.getLatitude),Some(status.getGeoLocation.getLongitude))
   
    }
    

  }// end getGeoLocationCoordinates method //
  
  
  
  
  
  
    /*.................................................................................................................*/
  /**
   * Method that
   * @return (Place latitude, Place longitude) if present, (None None) otherwise
   */
    def getPlaceCoordinates(place : twitter4j.Place) : (Option[Double], Option[Double], String) = {
    
    
  
    
    
    if (place == null)  (None, None, "place null")
    
    else {
       
      val boundingBoxCoordinates  = place.getBoundingBoxCoordinates
      val geometryCoordinates     = place.getGeometryCoordinates
      val containerPlace          = place.getContainedWithIn
        
      //check Bounding Box Coordinates
      if (boundingBoxCoordinates != null) (  Some(boundingBoxCoordinates.head.head.getLatitude),  Some(boundingBoxCoordinates.head.head.getLongitude),   "bounding box")
      else {  
              
              //check Geometry Coordinates
              if (geometryCoordinates != null)  (  Some(geometryCoordinates.head.head.getLatitude),  Some(geometryCoordinates.head.head.getLongitude),   "geometry")
              else{
                    
                  //check Container Place
                    if (containerPlace != null) getPlaceCoordinates(containerPlace.head)
                    else (None, None, "everything is null")
                  }
            }
        }
      
    
  }// end getPlaceCoordinates
    
    

  
  
  
   
}//end TweetStreamingApp class |||||||||||||






///////////////////////////////////////////// EXTRAAAAA STUFFF ////////////////////////////////////////////////////////



//optionally set word filters on input data
//val filters = new Array[String](0)




/*----------------------------------------------------
* Twitter Auth Settings
* >>>>>>> maybe in a .properties file?
*----------------------------------------------------

 //twitter auth magic configuration
 val consumerKey = "UhAKuJjpHsnursRKZnU5Rnv1M"                               // "hARrkNBpwsh8lLldQt7fTe4iM"
 val consumerSecret = "lklbynejp0GwDedxhU8J2bmj0rxZa0IwrUNH32HEBWQ2LQZnpg"   //"p0BRXCYEePUrJXPHQBxdIkP14idAYaSi934VJU2Hm2LBCUuqg0"
 val accessToken = "92626442-N7ozJgcKRnxkVPiSUVybjDHf5oWFA83l99M9j9v26"      // "72019464-oUaReZ3i91fcVKg3Y7mBOzxlNrNXMpa5sxOcIld3R"
 val accessTokenSecret = "u1sQeB8QpaSiKrp2VuwPnupSFdAbHOGiXDUBBzmywmlGc"    // "338I4ldbMc3CDpGYrpx5BuDfYbcAAZbJRDW86i9EY6Nwf"
 
 //setting system properties so that Twitter4j library can use general OAuth credentials
 System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
 System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
 System.setProperty("twitter4j.oauth.accessToken", accessToken)
 System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
 System.setProperty("twitter4j.http.proxyHost", "10.1.8.13")
 System.setProperty("twitter4j.http.proxyPort", "8080")*/





//val streamTweets = TwitterUtils.createStream(ssc, None)//, filters, StorageLevel.MEMORY_ONLY_SER)

//filtering by language and coordinates
/*val englishTweets = streamTweets
													/*.filter {t =>
																		 val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
																		 tags.contains("#bigdata")
																	 }
													*/
													.filter { status => status.getLang match{   case "it"     => true
																																			case ""       => true
																																			case default  => false  }
													}
													//.filter { status =>  myLocation.checkLocation(status) }
													 //.filter {status => status.getUser == "Paolo Gazzotti"}
	
		 */






/*...........................................
	* DataFrame definition
	*.........................................*/
/*
val schema = StructType(
													Array( StructField("tweet_id",      LongType,   true),
																 StructField("lang",          StringType, true),
																 
																 StructField("user_id",       LongType,   true),
																 StructField("user_name",     StringType, true),
																 
																 StructField("geo_latitude",   DoubleType, true),
																 StructField("geo_longitude",  DoubleType, true),
																 
																 StructField("place_latitude",   DoubleType, true),
																 StructField("place_longitude",  DoubleType, true),
																 
																 StructField("text",          StringType, true),
																 StructField("time",          LongType,   true),


														StructField("hashtagList", StringType, true)
																 
																)//end of Array definition
												 
																
												)//end of StructType




/*...........................................
* DataFrame definition
*.........................................*/
	
	
//ready tweet with correct info extraction
val readyTweetsDF = sqlContextHIVE.createDataFrame(
																										rdd.map(
																		
																														status =>   Row(
																																							status.getId,                         //tweet_id
																																							status.getLang,                       //lang
																																							
																																							status.getUser.getId,                 //user_id
																																							status.getUser.getName,               //user_name
																																							
																																							getGeoLocationCoordinates(status)._1, //gl_latitude
																																							getGeoLocationCoordinates(status)._2, //gl_longitude
																																							
																																							getPlaceCoordinates(status.getPlace)._1, //bb_latitude
																																							getPlaceCoordinates(status.getPlace)._2, //bb_longitude
																																							
																																							status.getText,                       //text
																																							status.getCreatedAt.getTime, //data
																															
																																							status.getHashtagEntities
																																										.map( x => x.getText)
																																										.mkString(" ")
																																										.toLowerCase()// hastag list separated by " "
																																							)
																																							
																														 ), schema)//end of createDataFrame

	@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@*/