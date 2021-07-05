name := "SaonGroupTest"

version := "0.1"

scalaVersion := "2.12.12"
val sparkVersion = "3.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.4",
  "net.java.dev.jets3t" % "jets3t" % "0.9.4",
  "com.amazon.redshift" % "redshift-jdbc42" % "2.0.0.4"
)