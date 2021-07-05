<h1>Saon Group Test</h1>

<h3>Data Profiling Results.</h3>

The data for each of the files contained on the S3 bucket was profiled using a 60% sample ratio to infer and compare the schemas. All the files have the same schema , the schema can be found in the file src/resources/data_profiling_results.txt.

<h3>Dataflow explanation and diagram.</h3>
Dataflow diagram

Plase visit: https://raw.githubusercontent.com/mguzman2016/SaonGroupTest/db8a5f9cee464e0da9cd1d79d95b72bb52c97f69/src/resources/DataFlow.png

The process is divided in 5 steps:
1.	Read Json Files. This step will read all of the files contained in the S3 Bucket into a dataframe (applicationsDf) which will be used on later steps.
2.	Build dimensions data. This step will build the necessary data for each of the dimensions according to the following logic:- Date Dimension. The date dimension is built by calculating all of the days between 2000-01-01 to 2031-01-01.- Companies Dimension. The companies dimension is built by selecting all of the distinct occurrences of company, city and sector and calculating the CRC32 checksum for those attributes by only keeping lowercase letters and digits.This is done to simulate a natural key which should be provided by the source system in order to make lookups to get the surrogate keys from each dimensional table faster.- Adverts Dimension. The adverts dimension is built by selecting all of the different occurrences of adverts (including all fields) and by filtering out null advert_ids.
3.	Insert dimensions data. Each of the dimensions is populated using the dimension data from the previous step. This is done to make redshift assign the surrogate keys for each record.
4.	Read dimensions from redshift. The dimensions and then queried from redshift to get the surrogate keys assigned to each record.
5.	Build fact table. After the dimensions have been populated the fact is built by exploding the “applicants” for each job and joining to each dimension by using their natural keys in order to get back the surrogate keys that are used to join the fact table and the dimension tables.

<h3>Data dictionary</h3>

<h4>dt_date and dt_date_advert. Dimensional tables for dates, dt_date_advert is a outrigger or "snowflaked" dimension as described on Kimball's data warehouse design</h4>
<table>
  <tr>
    <td>Field - dt_date</td>
    <td>Field - dt_advert</td>
    <td>Datatype</td>
    <td>Description</td>
  </tr>
  <tr>
    <td>dateid</td>
    <td>dateadvertid</td>
    <td>Integer</td>
    <td>Surrogate key</td>
  </tr>
  <tr>
    <td>day_date</td>
    <td>day_date</td>
    <td>Varchar</td>
    <td>Contains the date in yyyy-mm-dd format</td>
  </tr>
  <tr>
    <td>day</td>
    <td>day</td>
    <td>Integer</td>
    <td>Contains the day number</td>
  </tr>
  <tr>
    <td>month</td>
    <td>month</td>
    <td>Integer</td>
    <td>Contains the month number</td>
  </tr>
  <tr>
    <td>year</td>
    <td>year</td>
    <td>Integer</td>
    <td>Contains the year number</td>
  </tr>
</table>

<h4>dt_company. Dimensional table for companies</h4>

<table>
  <tr>
    <td>Field</td>
    <td>Datatype</td>
    <td>Description</td>
  </tr>
  <tr>
    <td>companyid</td>
    <td>Integer</td>
    <td>Surrogate key</td>
  </tr>
  <tr>
    <td>firmidcrc32</td>
    <td>Varchar</td>
    <td>Natural key. Contains the CRC32 of the lowercase concatenation for company, city and sector to simulate a natural key</td>
  </tr>
  <tr>
    <td>city</td>
    <td>Varchar</td>
    <td>Company city</td>
  </tr>
  <tr>
    <td>company</td>
    <td>Varchar</td>
    <td>Company name</td>
  </tr>
  <tr>
    <td>sector</td>
    <td>Varchar</td>
    <td>Company sector</td>
  </tr>
</table>

<h4>dt_advert. Dimensional table for adverts</h4>
<table>
  <tr>
    <td>Field</td>
    <td>Datatype</td>
    <td>Description</td>
  </tr>
  <tr>
    <td>advertid</td>
    <td>Integer</td>
    <td>Surrogate key</td>
  </tr>
  <tr>
    <td>dateadvertid</td>
    <td>Integer</td>
    <td>Foreign key used to join to dt_date_advert</td>
  </tr>
  <tr>
    <td>publicationdatetime</td>
    <td>Varchar</td>
    <td>Advert publication date time (as originally found in the data)</td>
  </tr>
  <tr>
    <td>id</td>
    <td>Varchar</td>
    <td>Advert id</td>
  </tr>
  <tr>
    <td>activedays</td>
    <td>Varchar</td>
    <td>Days for which an advert was active</td>
  </tr>
  <tr>
    <td>applyurl</td>
    <td>Varchar</td>
    <td>Url to apply for a job</td>
  </tr>
  <tr>
    <td>status</td>
    <td>Varchar</td>
    <td>Active or inactive advert</td>
  </tr>
  <tr>
    <td>benefits</td>
    <td>Varchar</td>
    <td>Benefits for the job posting</td>
  </tr>
  <tr>
    <td>title</td>
    <td>Varchar</td>
    <td>Name for the position</td>
  </tr>
</table>

<h4>ft_applicants. Fact table containing one row per each application to a job</h4>
<table>
  <tr>
    <td>Field</td>
    <td>Datatype</td>
    <td>Description</td>
  </tr>
  <tr>
    <td>applicantid</td>
    <td>Integer</td>
    <td>Surrogate key</td>
  </tr>
  <tr>
    <td>companyid</td>
    <td>Integer</td>
    <td>Foreign key used to join to dt_company</td>
  </tr>
  <tr>
    <td>advertid</td>
    <td>Integer</td>
    <td>Foreign key used to join to dt_advert</td>
  </tr>
  <tr>
    <td>dateid</td>
    <td>Integer</td>
    <td>Foreign key used to join to dt_date</td>
  </tr>
  <tr>
    <td>applicationdate</td>
    <td>Varchar</td>
    <td>Application date time (as originally found in the data)</td>
  </tr>
  <tr>
    <td>age</td>
    <td>Varchar</td>
    <td>Age of the applicant</td>
  </tr>
  <tr>
    <td>postingid</td>
    <td>Varchar</td>
    <td>Id for job posting</td>
  </tr>
  <tr>
    <td>firstname</td>
    <td>Varchar</td>
    <td>Applicant's first name</td>
  </tr>
  <tr>
    <td>lastname</td>
    <td>Varchar</td>
    <td>Applicant's first name</td>
  </tr>
  <tr>
    <td>skills</td>
    <td>Varchar</td>
    <td>Applicant's skills</td>
  </tr>
</table>

<h3> Dimensional model diagram </h3>
Please visit: https://raw.githubusercontent.com/mguzman2016/SaonGroupTest/db8a5f9cee464e0da9cd1d79d95b72bb52c97f69/src/resources/Dimensional%20Model.jpeg

<h3>Explanation of project structure</h3>

<h4>Application.ETLProcess.</h4>
This class in the application entrypoint. This will carry out the logic to execute the ETL Process, the class can be run in debugging (by setting the debugging flag on line 17 to true) mode which will only use a subset of the data and will also use temporary tables on the dev schema for testing purposes or it can make use of the complete dataset found on S3. Also spark logs can be suppressed (by setting the suppressMessages flag on line 19 to true). Ideally this parameters should be passed as program arguments on spark-submit.

<h4>Builders.SparkBuilder.</h4>
This class will build the Spark Session.

<h4>DBOperations.DatabaseOperations.</h4>
This class contains methods to insert, select and truncate tables on the database.

<h4>JsonOperations.ReadJsonData.</h4>
This class is used to read the data contained on S3.

<h4>JsonOperations.Schemas.</h4>
This class contains the schemas used to read the data from S3.

<h4>SparkOperations.Dimensions.</h4>
This class contains all the logic used to build the Dimensions (from spark side).

<h4>SparkOperations.Fact.</h4>
This class contains all the logic used to build the Fact Table (from spark side).
