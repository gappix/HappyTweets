package horizon

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf





/*||||||||||||||||||||||||||||||||||||||||     CONTEXT HANDLER     |||||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
 * This object instantiates all SPARK Contexts once, and then retrieves them with appropriate methods
 */
object ContextHandler {



  @transient lazy val log = LogManager.getLogger("horizon")





	/*<<<INFO>>>*/ log.info("creating SPARK Contexts...")


  //SPARK contexts creation
  private val conf = new SparkConf()
    .setAppName("2Horizons")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "10.1.2.172")





  //Kryo Options
    /*.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses( Array(
      classOf[scala.collection.mutable.WrappedArray[_]],
      classOf[Array[org.apache.spark.streaming.receiver.Receiver[_]]],
      classOf[org.apache.spark.sql.types.StructType],
      classOf[Array[org.apache.spark.sql.types.StructField]],
      classOf[org.apache.spark.sql.types.StructField],
      classOf[org.apache.spark.sql.types.StringType],
      classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
      //classOf[org.apache.spark.sql.types.StringType$],
      classOf[org.apache.spark.sql.types.Metadata],
      classOf[scala.collection.immutable.Map[_,_]],
      classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
      classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow],
      classOf[Array[Object]],
      classOf[org.apache.spark.unsafe.types.UTF8String],
      classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
      classOf[it.reti.spark.sentiment.SocketReceiver]    //classOf[org.apache.spark.streaming.twitter.TwitterReceiver]
    ))*/
    //.set("spark.kryo.registrationRequired","true")
    //Spark logger options
    //.set("spark.eventLog.enabled","true")
    //.set("spark.eventLog.compress","true")


    

                         
  /*
  private val sc = new SparkContext(conf)
  private val sqlContext = new SQLContext(sc)
  private val sqlContextHIVE = new HiveContext(sc)
  */





  /*<<<INFO>>>*/ log.info("Contexts created!")



  /*..................................................................................................................*/
  /**
    * method to
    * @return set Spark configurations
    */
  def  getConf = conf



  
  
  
}//end ContextHandler object |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||