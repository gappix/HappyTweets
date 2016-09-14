lazy val commonSettings = Seq(
																organization 	:= "it.reti",
																version			:= "0.0.1-SNAPSHOT",
																scalaVersion	:= "2.11.8"				)

lazy val root = (project in file(".")).settings( commonSettings : _*				)
																			.settings(	name := "GnipTryout"			)



// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.2"


// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.2"


// https://mvnrepository.com/artifact/org.apache.spark/spark-hive_2.11
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "1.6.2"


// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.6.2"


// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.2"


// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.6.2"


// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector_2.11
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "1.6.2"

