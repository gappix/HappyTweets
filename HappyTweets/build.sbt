lazy val commonSettings = Seq(
																organization 	:= "it.reti",
																version			:= "0.0.1-SNAPSHOT",
																scalaVersion	:= "2.11.8"				)

lazy val root = (project in file(".")).settings( commonSettings : _*				)
																			.settings(	name := "GnipTryout"			)


// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.2"
