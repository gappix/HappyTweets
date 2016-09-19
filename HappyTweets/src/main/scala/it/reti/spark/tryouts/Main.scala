package it.reti.spark.tryouts


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds



case class HBaseRecordAirline(col0: String,Year: Int,Quarter: Int,Month: Int,DayofMonth: Int,DayOfWeek: Int,FlightDate: Int,UniqueCarrier: String,AirlineID: String)






object Main{
  
  
  
  
  def main(args: Array[String])  { 
    
    
 /* 
  object HBaseRecordAirlineTest {def apply(i: Int): HBaseRecordAirline = {val s = s"""row${"%03d".format(i)}""" 
                                                                        HBaseRecordAirline(s,i,i,i,i,i,i,s,s)}}
  
  
  */
  
  val conf = new SparkConf().setAppName("BluePie_is_magic")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  
   val ssc = new StreamingContext(sc, Seconds(10))
  
  
  import sqlContext.implicits._

  
  
// Create a DStream that will connect to hostname:port, like localhost:9999
val lines = ssc.socketTextStream("localhost", 9999)
 // Split each line into words
val words = lines.flatMap(_.split(" ")) 
 // Count each word in each batch
val pairs = words.map(word => (word, 1))
val wordCounts = pairs.reduceByKey(_ + _)

// Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.print()


ssc.start()
ssc.awaitTermination()


}
  
}