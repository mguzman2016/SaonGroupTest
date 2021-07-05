
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, crc32, lower, regexp_replace, concat}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TestClassReadS3 extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("Scala-Testing-S3")
    .master("local")
    .getOrCreate()

  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIA3FHPVOOHL5GJMHWN")
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com")

  val advertSchema = new StructType().
    add("activeDays",LongType).
    add("applyUrl",StringType).
    add("id",StringType).
    add("publicationDateTime",StringType).
    add("status",StringType)
  val applicantsSchema = new StructType().
    add("age",LongType).
    add("applicationDate",StringType).
    add("firstName",StringType).
    add("lastName",StringType).
    add("skills",ArrayType(StringType))
  val dataSchema = new StructType().
    add("id",StringType).
    add("company",StringType).
    add("city",StringType).
    add("sector",StringType).
    add("title",StringType).
    add("benefits",ArrayType(StringType)).
    add("adverts",advertSchema).
    add("applicants",ArrayType(applicantsSchema))

  println("Read S3!")

  val df = spark.
    read.
    schema(dataSchema).
    json("s3n://technical-dev-test/raw/jobs/1.json")

  df.show()

  val companies = df.select(
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

  companies.
    write.
    format("jdbc").
    option("url","jdbc:redshift://redshift-cluster-1.csvoccsyss6k.us-east-2.redshift.amazonaws.com:5439/dev;user=awsuser;password=S40nGr0up!;readOnly=false").
    option("driver","com.amazon.redshift.jdbc42.Driver").
    option("dbtable","public.dt_company_tmp").
    mode("overwrite").
    save()

}
