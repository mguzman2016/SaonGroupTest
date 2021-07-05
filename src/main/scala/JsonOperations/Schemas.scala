package JsonOperations

import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}

class Schemas {

  //Adverts Schema
  val advertSchema = new StructType().
    add("activeDays",LongType).
    add("applyUrl",StringType).
    add("id",StringType).
    add("publicationDateTime",StringType).
    add("status",StringType)

  //Applicants Schema
  val applicantsSchema = new StructType().
    add("age",LongType).
    add("applicationDate",StringType).
    add("firstName",StringType).
    add("lastName",StringType).
    add("skills",ArrayType(StringType))

  //Json complete schema, uses both adverts and applicants schema.
  val dataSchema = new StructType().
    add("id",StringType).
    add("company",StringType).
    add("city",StringType).
    add("sector",StringType).
    add("title",StringType).
    add("benefits",ArrayType(StringType)).
    add("adverts",advertSchema).
    add("applicants",ArrayType(applicantsSchema))

}
