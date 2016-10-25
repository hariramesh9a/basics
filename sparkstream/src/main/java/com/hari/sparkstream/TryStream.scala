package com.hari.sparkstream
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType }
import org.apache.spark.SparkConf
object TryStream {
  
  
  def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Active Archive").setMaster("local[*]")
    val sc = new SparkContext(conf)
        val sqlCont = new HiveContext(sc)
        val mySchema=StructType(Array(
         StructField("Row_ID",StringType,false),
         StructField("Name",StringType,false),
         StructField("Desc",StringType,false)))
    
      val myData = sqlCont.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "false")
      .option("lineSep","|")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(mySchema)
      .load("src/main/java/badcsv.dat")
      myData.show()
    
    
  }
}