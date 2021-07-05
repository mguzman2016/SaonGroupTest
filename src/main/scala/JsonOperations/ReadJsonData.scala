package JsonOperations

import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadJsonData extends Schemas{

  /*
  * This method will read all the json data on the S3 Bucket
  * Parameters: spark: SparkSession object
  *             dataLocation: Data Location on the S3 Bucket
  *             testing: This parameter will limit the data returned to 20 records for testing purposes
  * */
  def readJsonData(spark: SparkSession, dataLocation: String, testing: Boolean): DataFrame = {
    val df = spark.
      read.
      schema(dataSchema).
      json(dataLocation)

    if(!testing){
      df
    } else{
      df.limit(20)
    }

  }

}
