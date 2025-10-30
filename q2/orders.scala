import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Q2 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession()
        import spark.implicits._
        val (c, o, i) = getDF(spark)
        val counts = doOrders(c,o,i)
        saveit(counts, "dataframes_q2")  // save the rdd to your home directory in HDFS
    }


    def doOrders(customers: DataFrame, orders: DataFrame, items: DataFrame): DataFrame = {
        val orders_2 = orders
          .selectExpr("StockCode", "Quantity", "CustomerID")
        val items_2 = items
          .selectExpr("StockCode", "UnitPrice")

        val join_orders = customers
          .join(orders_2, "CustomerID")
        val join_orders_items = join_orders
          .join(items_2, "StockCode")

        val withtprice= join_orders_items
          .withColumn("TotalPrice", col("UnitPrice") * col("Quantity"))

        val aggregated = withtprice
          .groupBy("Country")
          .agg(sum("TotalPrice").as("Total"))

        val selectTotal = aggregated
          .selectExpr("Country", "Total")

        selectTotal

    }

    def getDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        val customerSchema = new StructType()
          .add("CustomerID", StringType,true)
          .add("Country", StringType,true)

        val ordersSchema = new StructType()
          .add("InvoiceNo", StringType,true)
          .add("StockCode", StringType,true)
          .add("InvoiceDate", StringType, true)
          .add("CustomerID", StringType, true)
          .add("Quantity", IntegerType,true)

        val itemsSchema = new StructType()
          .add("StockCode", StringType,true)
          .add("Description",StringType,true)
          .add("UnitPrice", DoubleType, true)

        val customersPath = "/datasets/orders/customers.csv"
        val ordersPath = "/datasets/orders/orders.csv"
        val itemsPath = "/datasets/orders/items.csv"

        val customers = spark.read.format("csv").schema(customerSchema)
          .option("header", "true")
          .option("delimiter", "\t")
          .option("mode", "dropMalformed").load(customersPath)


        val orders = spark.read.format("csv").schema(ordersSchema)
          .option("header", "true")
          .option("delimiter", "\t")
          .option("mode", "dropMalformed").load(ordersPath)

        val items = spark.read.format("csv").schema(itemsSchema)
          .option("header", "true")
          .option("delimiter", "\t")
          .option("mode", "dropMalformed").load(itemsPath)
        val cleanedCust = customers.na.fill(
          "blank",
          Seq("CustomerID"))
        val cleanedOrd = orders.na.fill(
          "blank",
          Seq("CustomerID"))

        (cleanedCust,cleanedOrd,items)

    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        spark
    }

    def getTestDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        // don't forget the spark.implicits
        import spark.implicits._
        // Test DataFrame for customers
        val customers = Seq(
          ("12346", "Bahrain"),
          ("12347", "United Kingdom"),
          (null, "Canada")
        ).toDF("CustomerID", "Country")
         
        // Test DataFrame for orders
        val orders = Seq(
          ("542420", "10002", 1, "1/27/2011 18:05", "12346"),
          ("542421", "10120", 2, "1/27/2011 19:33", "12347"),
          ("542422", "10123C", 3, "1/27/2011 20:15", null)
        ).toDF("InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "CustomerID")

        // Test DataFrame for items
        val items = Seq(
          ("10002", "Inflatable Political Globe", 1.66),
          ("10120", "Doggy Rubber", 0.21),
          ("10123C", "Hearts Wrapping Tape", 0.65)
        ).toDF("StockCode", "Description", "UnitPrice")

        (customers, orders, items)
        // return 3 dfs (customers, orders, items)
    }
    def expectedOutput(spark: SparkSession) = {
        import spark.implicits._
        Seq(
            ("Bahrain", 1.66),
            ("United Kingdom", 0.42),
            ("Canada", 1.95)
           ).toDF("Country", "TotalSpent")

    }

    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)

    }

}