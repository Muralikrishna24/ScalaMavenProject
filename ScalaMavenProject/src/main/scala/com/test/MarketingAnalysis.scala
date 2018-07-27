package com.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object MarketingAnalysis {
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "C:\\murali\\HadoopOnWindows\\Hadoop");
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local").getOrCreate()
    val inputFile = spark.read.format("csv").option("header","true").option("delimiter",";").option("inferSchema","true").load("C:\\murali\\MarketingAnalysis\\Project 1_dataset_bank-full.csv");
   
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    println("Schema of DF:"+inputFile.printSchema())
    println("First Record:"+inputFile.first)
    println("Marketing Success Rate:"+inputFile.filter($"y" === "yes").count())
    println("Marketing Failure Rate:"+inputFile.filter($"y" === "no").count())
    
    val newInputFileDF = inputFile.withColumn("targetStatus",when($"age">25 and $"age"<35,"yes").otherwise("no"))
    newInputFileDF.take(5).foreach(println)
  }
} 