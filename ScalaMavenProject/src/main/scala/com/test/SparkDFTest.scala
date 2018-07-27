package com.test
import org.apache.spark.sql.SparkSession

object SparkDFTest {
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "C:\\murali\\HadoopOnWindows\\Hadoop");
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local").getOrCreate()
    val inputFile = spark.read.format("csv").option("header","false").load("C:\\murali\\emp.csv");
    println(inputFile.show)
  }
  
}