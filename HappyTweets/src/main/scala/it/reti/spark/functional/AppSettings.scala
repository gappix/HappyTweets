package it.reti.spark.functional

import org.apache.spark.SparkConf

/*||||||||||||||||||||||||||||||||||||||||||||     TITLE    |||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/**
	* Welcome app screen print out
	* Created by gazzopa1 on 10/10/2016.
	*/
object AppSettings {




	/*..................................................................................................................*/
	/**
		* method to print welcome app name on launch
		*/
	def print_title = {


		print("""




                ('-. .-.             _  .-')             .-') _                  .-') _   .-')
               ( OO )  /            ( \( -O )           (  OO) )                ( OO ) ) ( OO ).
 .-----.       ,--. ,--. .-'),-----. ,------.  ,-.-') ,(_)----. .-'),-----. ,--./ ,--,' (_)---\_)
/ ,-.   \      |  | |  |( OO'  .-.  '|   /`. ' |  |OO)|       |( OO'  .-.  '|   \ |  |\ /    _ |
'-'  |  |      |   .|  |/   |  | |  ||  /  | | |  |  \'--.   / /   |  | |  ||    \|  | )\  :` `.
   .'  /       |       |\_) |  |\|  ||  |_.' | |  |(_/(_/   /  \_) |  |\|  ||  .     |/  '..`''.)
 .'  /__       |  .-.  |  \ |  | |  ||  .  '.',|  |_.' /   /___  \ |  | |  ||  |\    |  .-._)   \
|       |      |  | |  |   `'  '-'  '|  |\  \(_|  |   |        |  `'  '-'  '|  | \   |  \       /
`-------'      `--' `--'     `-----' `--' '--' `--'   `--------'    `-----' `--'  `--'   `-----'






""")

	}//end printTitle //






	/*..................................................................................................................*/
	/**
		* function to get all code for spark context configuration grouped all toghether
		* @return SparkConf class ready to be passed to a SparkContext
		*/
	def spark_configuration: SparkConf = {


		val conf =  new SparkConf().setAppName("2 Horizons")
															//Cassandra connection
															.set("spark.cassandra.connection.host", "10.1.2.172")
															//Kryo Options
															.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
															.registerKryoClasses( Array(
//																classOf[scala.collection.mutable.WrappedArray[_]],
//																classOf[Array[org.apache.spark.streaming.receiver.Receiver[_]]],
//																classOf[org.apache.spark.sql.types.StructType],
//																classOf[Array[org.apache.spark.sql.types.StructField]],
//																classOf[org.apache.spark.sql.types.StructField],
//																classOf[org.apache.spark.sql.types.StringType],
//																classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
//																//classOf[org.apache.spark.sql.types.StringType$],
//																classOf[org.apache.spark.sql.types.Metadata],
//																classOf[scala.collection.immutable.Map[_,_]],
//																classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
//																classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow],
//																classOf[Array[Object]],
//																classOf[org.apache.spark.unsafe.types.UTF8String],
//																classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
//																classOf[it.reti.spark.sentiment.SocketReceiver]
																//classOf[org.apache.spark.streaming.twitter.TwitterReceiver]
															))
															//.set("spark.kryo.registrationRequired","true")
															//Spark logger options
															//.set("spark.eventLog.enabled","true")
															//.set("spark.eventLog.compress","true")


		conf

	}//end sparkConfiguration method //







	/*..................................................................................................................*/
	/**
		* method that
		* @return tweets source file name and path
		*/
	def input_file = "/RawTweets.json"






}//end Title object ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
