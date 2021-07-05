package Application

import Builders.SparkBuilder
import DBOperations.DatabaseOperations
import SparkOperations.{Dimensions, Fact}
import JsonOperations.ReadJsonData
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col

object ETLProcess extends SparkBuilder{

  def main(args: Array[String]): Unit = {

    //Global execution variables
    val rootLogger = Logger.getRootLogger
    //Setting this to true will make use only a subset of the data to speed up testing process and will show dataframes
    val debugging = false
    //Setting this to true will suppress spark logs, but will show "WARN" application log messages
    val suppressMessages = true
    //String to connect to redshift
    val cluster = "redshift-cluster-1.csvoccsyss6k.us-east-2.redshift.amazonaws.com"
    val port = "5439"
    val db = "dev"
    val user = "awsuser"
    val password = "S40nGr0up!"
    val jdbcString = s"jdbc:redshift://${cluster}:${port}/${db};user=${user};password=${password}"

    //Table names
    val schema = if(!debugging){ "public" }else{ "dev" }
    val dateTableName = if(!debugging){ "dt_date" }else{ "tmp_dates_dt" }
    val datesAdvertTableName = if(!debugging){ "dt_date_advert" }else{ "tmp_dates_advert_dt" }
    val companyTableName = if(!debugging){ "dt_company" }else{ "tmp_companies_dt" }
    val advertTableName = if(!debugging){ "dt_advert" }else{ "tmp_adverts_dt" }
    val factTableName = if(!debugging){ "ft_applicants" }else{ "tmp_fact_tbl" }
    //Location to read jsons from
    val readLocation = if(!debugging){ "s3n://technical-dev-test/raw/jobs/" }else{ "s3n://technical-dev-test/raw/jobs/1.json" }

    //Build spark
    val spark = super.buildSpark(suppressMessages)

    //Step 1: Read JSON Files.
    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Reading JSON Data")
    }

    val jsonReader = new ReadJsonData
    val applicationsDf = jsonReader.readJsonData(spark,readLocation,debugging)

    if(debugging){
      applicationsDf.show
    }
    //End of Step 1.

    //Step 2: Build dimensions data
    val dimensions = new Dimensions

    //Start of DATE DIMENSION
    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Building Date Dimension")
    }

    val datesDf = dimensions.buildDateDimension(spark)

    if(debugging){
      datesDf.show
    }
    //End of DATE DIMENSION

    //Start of COMPANIES DIMENSION
    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Building Company Dimension")
    }

    val companiesDf = dimensions.buildCompaniesDimension(applicationsDf)

    if(debugging){
      companiesDf.show
    }
    //End of COMPANIES DIMENSION

    //Start of ADVERTS DIMENSION
    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Building Adverts Dimension")
    }

    val advertsDf = dimensions.buildAdvertsDimension(applicationsDf, datesDf)

    if(debugging){
      advertsDf.show
    }
    //End of ADVERTS DIMENSION
    //End of Step 2.

    //Step 3. Insert dimensions
    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Inserting dimensions data")
    }

    val database = new DatabaseOperations

    //Dates DT
    database.truncateTable(schema,dateTableName,jdbcString)
    database.insertDfIntoDB(if(!debugging){ datesDf }else{ datesDf.limit(100) },schema,dateTableName,jdbcString)
    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Inserted dates data")
    }

    //Adverts Dates DT
    val datesDfColumns = Seq(
      col("dateid").as("dateadvertid"),
      col("day_date"),
      col("day"),
      col("month"),
      col("year")
    )
    database.truncateTable(schema,datesAdvertTableName,jdbcString)
    database.insertDfIntoDB(datesDf.select(datesDfColumns: _*).limit(100),schema,datesAdvertTableName,jdbcString)
    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Inserted adverts dates data")
    }

    //Companies DT
    database.truncateTable(schema,companyTableName,jdbcString)
    database.insertDfIntoDB(companiesDf,schema,companyTableName,jdbcString)
    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Inserted companies data")
    }

    //Adverts DT
    database.truncateTable(schema,advertTableName,jdbcString)
    database.insertDfIntoDB(advertsDf,schema,advertTableName,jdbcString)
    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Inserted adverts data")
    }
    //End of step 3.

    //Step 4. Read Companies and adverts tables to get the computed database primary key

    //Companies
    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Fetching companies from DB")
    }
    val companiesColumns = Seq(
      col("companyid").as("company_companyid"),
      col("firmidcrc32").as("company_firmcrc32")
    )
    val dbCompaniesDf = database.readDfFromDB(spark,schema,companyTableName,jdbcString,companiesColumns)
    if(debugging){
      dbCompaniesDf.show
    }

    //Adverts
    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Fetching adverts from DB")
    }
    val advertsColumns = Seq(
      col("advertid").as("adverts_advertid"),
      col("id").as("adverts_id")
    )
    val dbAdvertsDf = database.readDfFromDB(spark,schema,advertTableName,jdbcString,advertsColumns)
    if(debugging){
      dbAdvertsDf.show
    }
    //End of step 4.

    //Step 5. Build fact dataframe

    val fact = new Fact

    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Building fact DF")
    }

    val factDataframe = fact.buildApplicantsDf(
      applicationsDf,
      datesDf,
      dbCompaniesDf,
      dbAdvertsDf
    )

    if(debugging){
      factDataframe.show
    }

    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Inserting fact DF "+factDataframe.count)
    }
    database.truncateTable(schema,factTableName,jdbcString)
    database.insertDfIntoDB(factDataframe,schema,factTableName,jdbcString)

    if(suppressMessages){
      rootLogger.warn("APPLICATION DEBUG: Inserted Fact DF")
    }

    spark.stop()

    //End of step 5

  }

}
