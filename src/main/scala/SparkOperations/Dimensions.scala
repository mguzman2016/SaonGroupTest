package SparkOperations

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array_join, col, concat, crc32, date_format, from_unixtime, lower, regexp_replace}

class Dimensions {

  /*
  * This method will return all the dates between 2000-01-01 to 2030-01-01
  * Parameters: spark: SparkSession object
  * */
  def buildDateDimension(spark: SparkSession): DataFrame = {
    //Get all the days between two dates
    spark.sql(
      """
        |SELECT
        |     monotonically_increasing_id() + 1 as dateid,
        |     date_format(to_date(dates),'yyyy-MM-dd') as day_date,
        |     day(dates) as day,
        |     month(dates) as month,
        |     year(dates) as year
        |FROM(
        |       SELECT explode(sequence(to_date('2000-01-01'),to_date('2031-01-01'))) as dates
        |)
        |ORDER BY
        |     to_date(dates)
        |""".stripMargin)
  }

  /*
  * This method will get all of the unique companies based on company name, city and sector to avoid data
  * duplication on the companies dimension.
  * Parameters: jsonDf: dataframe that contains the data read from the S3 Buckets
  * */
  def buildCompaniesDimension(jsonDf: DataFrame): DataFrame= {
    jsonDf.select(
      col("company"),
      col("city"),
      col("sector")
    ).na.fill("no_data",Seq("company","city","sector")).
      select(
        crc32(lower(regexp_replace(concat(col("company"),col("city"),col("sector")),"[^A-Za-z0-9]+",""))).as("firmidcrc32"),
        col("city"),
        col("company"),
        col("sector")
      ).distinct()
  }

  /*
  * This method will get all of the unique adverts.
  * Parameters: jsonDf: dataframe that contains the data read from the S3 Buckets
  *             datesDf: dataframe that contains the dates
  * */
  def buildAdvertsDimension(jsonDf: DataFrame, datesDf: DataFrame): DataFrame= {

    val adverts = jsonDf.select(
      col("adverts.publicationDateTime").as("adverts_publicationDateTime"),
      date_format(from_unixtime(col("adverts.publicationDateTime")),"yyyy-MM-dd").as("adverts_publicationDateTimeFormat"),
      col("adverts.id").as("adverts_id"),
      col("adverts.activeDays").as("adverts_activeDays"),
      col("adverts.applyUrl").as("adverts_applyUrl"),
      col("adverts.status").as("adverts_status"),
      array_join(col("benefits"),"|").as("adverts_benefits"),
      col("title")
    ).where("adverts_id is not null").
      distinct()

    val advertsJoinedDates = adverts.join( datesDf, adverts("adverts_publicationDateTimeFormat") === datesDf("day_date") , "inner")

    val advertsToSave = advertsJoinedDates.select(
      col("dateid").as("dateadvertid"),
      col("adverts_publicationDateTime").as("publicationdatetime"),
      col("adverts_id").as("id"),
      col("adverts_activeDays").as("activedays"),
      col("adverts_applyUrl").as("applyurl"),
      col("adverts_status").as("status"),
      col("adverts_benefits").as("benefits"),
      col("title")
    ).na.fill("no_data",Seq("dateadvertid","publicationdatetime","id","activedays","applyurl","status","benefits","title"))

    advertsToSave
  }

}
