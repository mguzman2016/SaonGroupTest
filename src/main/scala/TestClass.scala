
import org.apache.spark.sql.{SparkSession}

//This class is to test scala dependencies
object TestClass extends App{

  println("Hello world!")

  val spark = SparkSession.builder()
    .appName("Scala-Testing")
    .master("local")
    .getOrCreate()

  val initialTable = spark.range(1, 10000000)
  initialTable.show

}
