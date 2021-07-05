package SparkOperations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array_join, col, crc32, date_format, explode, from_unixtime, lower, regexp_replace, concat}

class Fact {

  def buildApplicantsDf(jsonDf: DataFrame, dateDf: DataFrame, companyDf: DataFrame, advertsDf: DataFrame):
  DataFrame = {
    val DfApplicants = jsonDf.
      select(
        col("company"),
        col("city"),
        col("sector"),
        col("adverts.id").as("advert_id"),
        explode(col("applicants")).as("applicants_unnest"),
        col("id")
      ).select(
      col("company").as("df_applicants_company"),
      col("city").as("df_applicants_city"),
      col("sector").as("df_applicants_sector"),
      col("advert_id").as("df_applicants_advert_id"),
      col("applicants_unnest.applicationDate").as("df_applicants_application_date"),
      date_format(from_unixtime(col("applicants_unnest.applicationDate")),"yyyy-MM-dd").as("df_applicants_application_date_parsed"),
      col("applicants_unnest.age").as("df_applicants_age"),
      col("id").as("df_applicants_posting_id"),
      col("applicants_unnest.firstName").as("df_applicants_firstName"),
      col("applicants_unnest.lastName").as("df_applicants_lastName"),
      array_join(col("applicants_unnest.skills"),"|").as("df_applicants_skills")
    ).
      na.
      fill("no_data",Seq(
        "df_applicants_company",
        "df_applicants_city",
        "df_applicants_sector",
        "df_applicants_posting_id",
        "df_applicants_application_date",
        "df_applicants_advert_id",
        "df_applicants_firstName",
        "df_applicants_lastName",
        "df_applicants_skills",
        "df_applicants_application_date_parsed"
      )
      ).na.
      fill(0,Seq("df_applicants_age")).
      select(
        crc32(lower(regexp_replace(concat(col("df_applicants_company"),col("df_applicants_city"),col("df_applicants_sector")),"[^A-Za-z0-9]+",""))).as("df_applicants_company_firmidcrc32"),
        col("df_applicants_company"),
        col("df_applicants_city"),
        col("df_applicants_sector"),
        col("df_applicants_advert_id"),
        col("df_applicants_application_date"),
        col("df_applicants_application_date_parsed"),
        col("df_applicants_age"),
        col("df_applicants_posting_id"),
        col("df_applicants_firstName"),
        col("df_applicants_lastName"),
        col("df_applicants_skills")
      )

    val joinedAdverts = DfApplicants.join(advertsDf, DfApplicants("df_applicants_advert_id") === advertsDf("adverts_id"), "inner")

    val joinedAdvertsCompany = joinedAdverts.join(companyDf, DfApplicants("df_applicants_company_firmidcrc32") === companyDf("company_firmcrc32"), "inner")

    val joinedAdvertsCompanyDate = joinedAdvertsCompany.join(dateDf, joinedAdvertsCompany("df_applicants_application_date_parsed") === dateDf("day_date"), "left")

    joinedAdvertsCompanyDate.
      select(
        col("company_companyid").as("companyid"),
        col("adverts_advertid").as("advertid"),
        col("dateid"),
        col("df_applicants_application_date").as("applicationdate"),
        col("df_applicants_age").as("age"),
        col("df_applicants_posting_id").as("postingid"),
        col("df_applicants_firstName").as("firstname"),
        col("df_applicants_lastName").as("lastname"),
        col("df_applicants_skills").as("skills")
      ).na.
      fill(0,Seq(
        "dateid"
      )
      )

  }

}
