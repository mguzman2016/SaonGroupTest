package Builders

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class SparkBuilder {

  /**
   * This method will build a spark session
   *
   * Parameters: suppressMessages: if set to true will make spark messages silent.
   */
  def buildSpark(suppressMessages: Boolean): SparkSession = {

    if(suppressMessages){
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("akka").setLevel(Level.ERROR)
    }

    val spark = SparkSession.builder()
    .appName("SaonTest")
    .master("local")
    .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIA3FHPVOOHL5GJMHWN")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com")

    spark
  }

}
