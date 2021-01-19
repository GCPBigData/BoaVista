package solutions.csv

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
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
    .load("src/main/resources/etl/price_quote.csv")
    .createOrReplaceTempView("priceView")

  // Reading CSV comp_boss.csv
  val compDf = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/etl/comp_boss.csv")
    .createOrReplaceTempView("compView")

  // Reading CSV bill_of_materials.csv
  val billDf = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/etl/bill_of_materials.csv")
    .createOrReplaceTempView("billView")

  //Seleciona as Views
  val dfSQLPrice = spark.sql("SELECT * FROM priceView")
  val dfSQLComp = spark.sql("SELECT * FROM compView")
  val dfSQLBill = spark.sql("SELECT * FROM billView")

  val dfSQLBilPrice = spark.sql("SELECT b.*, p.* FROM priceView p INNER JOIN billView b ON b.tube_assembly_id = p.tube_assembly_id")
      dfSQLBilPrice.show()

  val dfSQLBillComp = spark.sql("SELECT b.*, p.* FROM compView p INNER JOIN billView b ON b.component_id_1 = p.component_id")
      dfSQLBillComp.show()

  //Converte em parquet
  dfSQLPrice.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/etl/bill_of_materials.parquet")
  dfSQLComp.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/etl/comp_boss.parquet")
  dfSQLBill.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/etl/price_quote.parquet")
  dfSQLBillComp.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/etl/bill_comp.parquet")

  sc.stop

}
