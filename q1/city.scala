import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q1 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark) 
        val counts = doCity(mydf)
        saveit(counts, "dataframes_q1")  // save the rdd to your home directory in HDFS
    }

    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {case Some(y) => y.split(" ").size; case None => 0}})
        spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
    }

    def doCity(input: DataFrame): DataFrame = {
        val filter_1 = input.selectExpr("city","state_abbreviation","population","zip_codes")
        val grouped = filter_1.groupBy("state_abbreviation")
        val aggregated = grouped.agg(
          count("city").as("num_cities"),
          sum("population").as("total_population"),
          max(expr("zipCounter(zip_codes)")).as("max_zip_codes"))
        aggregated
    }

    def getDF(spark: SparkSession): DataFrame = {
        // when spark reads dataframes from csv, when it encounters errors it just creates lines with nulls in 
        // some of the fields, so you will have to check the slides and the df to see where the nulls are and
        // what to do about them
        val citiesSchema = new StructType()
          .add("city", StringType, true)
          .add("state_abbreviation", StringType, true)
          .add("state", StringType, true)
          .add("county", StringType, true)
          .add("population", LongType, true)
          .add("zip_codes", StringType, true)
          .add("ID", StringType, true)
        val data_csv = "/datasets/cities/cities.csv"
        val dfcsv = spark.read.format("csv").schema(citiesSchema)
          .option("header", "true")
          .option("delimiter", "\t")
          .option("mode", "dropMalformed").load(data_csv)

        val no_null = dfcsv.filter(col("state_abbreviation").isNotNull)

        no_null
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark) // tells the spark session about the UDF
        spark
    }

    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._ //so you can use .toDF
        // check slides carefully. note that you don't need to add headers, unlike RDDs
        Seq(
            ("Chicago","IL", "Illinois", "Cook", 10000, "60602 45678", "1"),
            ("Boston","MA", "Massachusetts", "Suffolk", 20000, "02108 23980", "2"),
            ("Springfield","IL", "Illinois", "Sangamon", 30000, "94103 29902", "3")
          ).toDF("city", "state_abbreviation","state", "county", "population", "zip_codes", "ID")
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._ //so you can use .toDF
        Seq( // Use a sequence to define the expected output
            ("MA", 1, 20000,2),
            ("IL", 2, 40000,2)
           ).toDF("state_abbreviation", "num_cities", "total_population", "max_zip_codes")
        
    }

    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)
    }

}