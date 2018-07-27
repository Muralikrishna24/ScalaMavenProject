package com.test

import org.apache.spark.sql.SparkSession

object MarketingAnalysis1 {
  case class Bank(age:Int, job:String, marital:String, education:String, defaultStatus:String, balance:Int, housing:String, loan:String, contact:String, day:String, month:String, duration:Int, campaign:Int, pdays:String, previous:Int, poutcome:String, y:String)
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\murali\\HadoopOnWindows\\Hadoop");
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local").getOrCreate()
    val sc=spark.sparkContext
    val inputFile = sc.textFile("C:\\murali\\MarketingAnalysis\\Project 1_dataset_bank-full.csv");
    val input_split = inputFile.map(x=>x.split(";"))
    val bankRDD = input_split.map(x=>Bank(x(0).toInt, x(1), x(2), x(3), x(4), x(5).toInt, x(6), x(7), x(8), x(9), x(10), x(11).toInt, x(12).toInt, x(13), x(14).toInt, x(15), x(16)))
    import spark.implicits._
    val bankDF = bankRDD.toDF()
    println("Schema of DF:"+bankDF.printSchema())
    println("First Record:"+bankDF.first)
    println("Marketing Success Rate:"+bankDF.filter($"y" === "yes").count())
    println("Marketing Failure Rate:"+bankDF.filter($"y" === "no").count())
   }
}