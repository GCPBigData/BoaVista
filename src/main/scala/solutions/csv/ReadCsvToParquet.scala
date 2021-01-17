package solutions.csv

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
/**
  * A Spark schema structure that describes price quote DataFrame.
  *
  *
  * tube_assembly_id TA-00002
  * supplier S-0066
  * quote_date 2013-07-07
  * annual_usage 0
  * min_order_quantity 0
  * bracket_pricing Yes
  * quantity 1
  * cost 21.9059330191461
  *
  *
  * @author Jose R F Junior - web2ajax@gmail.com
  */

object ReadCsvToParquet extends App {

   /**
    * This creates a SparkSession, which will be used to operate on the DataFrames that we create.
    */

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // Contexto
  val sc = spark.sparkContext

  // Reading CSV price_quote.csv
  val priceDf = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/price_quote.csv")
    .createOrReplaceTempView("priceView")

  // Reading CSV comp_boss.csv
  val compDf = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/comp_boss.csv")
    .createOrReplaceTempView("compView")

  // Reading CSV bill_of_materials.csv
  val billDf = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/bill_of_materials.csv")
    .createOrReplaceTempView("billView")

  //Seleciona as Views
  val dfSQLPrice = spark.sql("SELECT * FROM priceView")
  val dfSQLComp = spark.sql("SELECT * FROM compView")
  val dfSQLBill = spark.sql("SELECT * FROM billView")

  val dfSQLFull = spark.sql("" +
      "SELECT tube_assembly_id, quantity, cost FROM priceView UNION ALL " +
      "SELECT tube_assembly_id, component_id_1, quantity_1 FROM billView ORDER BY tube_assembly_id")
  dfSQLFull.show()
  //Cria uma view
   dfSQLFull.createOrReplaceTempView("dfSQLFull")

  // Data Science
  // Min
  val minRatingDf = dfSQLPrice.select(min(col("quantity")))
  dfSQLPrice.selectExpr("min(quantity)").show()

  // Max
  val maxRatingDf = dfSQLPrice.select(max(col("quantity")))
  dfSQLPrice.selectExpr("max(quantity)").show()

  // Avg
  dfSQLPrice.select(avg(col("quantity"))).show()
  dfSQLPrice.selectExpr("avg(quantity)").show()

  //Mean
  dfSQLPrice.select(
    mean(col("quantity")),
    stddev(col("quantity"))
  ).show()

  //Converte em parquet
  dfSQLPrice.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/bill_of_materials.parquet")
  dfSQLComp.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/comp_boss.parquet")
  dfSQLBill.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/price_quote.parquet")

  sc.stop

}
