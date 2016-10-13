name := "HappyTweet"

version := "1.0"

scalaVersion := "2.11.8"




/*
// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies ++=   Seq( "org.apache.spark" % "spark-core_2.11" % "2.0.0",
															"org.apache.spark" %% "spark-sql_2.11" % "2.0.0")//														"org.apache.spark" %% "spark-hive" % "2.0.0"

*/

val sparkVersion = "2.0.1"


resolvers ++= Seq(
	"apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-sql" % sparkVersion,
	"org.apache.spark" %% "spark-mllib" % sparkVersion,
	"org.apache.spark" %% "spark-streaming" % sparkVersion,
	"org.apache.spark" %% "spark-hive" % sparkVersion,
	"com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3"

)
