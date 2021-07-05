package DBOperations

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.sql.DriverManager

class DatabaseOperations {

  /*
  * This method will insert a dataframe into the database.
  * Parameters: dataFrame: Dataframe to insert into the database.
  *             schema: Schema in which the table is located
  *             tableName: Name of the table
  *             jdbcString: String containing the DB Data
  * */
  def insertDfIntoDB (dataFrame: DataFrame,schema: String, tableName: String, jdbcString: String) : Unit = {
    dataFrame.
      write.
      format("jdbc").
      option("url",jdbcString).
      option("driver","com.amazon.redshift.jdbc42.Driver").
      option("dbtable",schema+"."+tableName).
      mode("append").
      save()
  }

  /*
  * This method will read a database table and return it as a dataframe
  * Parameters: spark: SparkSession object
  *             schema: Schema in which the table is located
  *             tableName: Name of the table
  *             jdbcString: String containing the DB Data
  *             columns: List of columns to be returned from the table
  * */
  def readDfFromDB (spark: SparkSession, schema: String, tableName: String, jdbcString: String, columns: Seq[Column]):
    DataFrame = {

    spark.read.
      format("jdbc").
      option("url", jdbcString).
      option("dbtable", schema+"."+tableName).
      option("driver","com.amazon.redshift.jdbc42.Driver").
      load().
      select(columns: _*)

    }

  /*
  * This method will truncate a database table
  * Parameters: tableName: Name of the table
  *             jdbcString: String containing the DB Data
  * */
  def truncateTable (tableName: String, jdbcString: String) : Unit = {
      val connection = DriverManager.getConnection(jdbcString)
      connection.setAutoCommit(true)
      val statement = connection.createStatement()
      statement.execute(s"TRUNCATE TABLE $tableName")
  }

}
