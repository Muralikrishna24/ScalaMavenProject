package com.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkScalaSample {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\murali\\HadoopOnWindows\\Hadoop");
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local").getOrCreate()
    
    //create a df using seq
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.Column
    val namesDF = Seq(("Yoda",             "Obi-Wan Kenobi"),
             ("Anakin Skywalker", "Sheev Palpatine"),
             ("Luke Skywalker",   "Han Solo, Leia Skywalker"),
             ("Leia Skywalker",   "Obi-Wan Kenobi"),
             ("Sheev Palpatine",  "Anakin Skywalker"),
             ("Han Solo",         "Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi, Chewbacca"),
             ("Obi-Wan Kenobi",   "Yoda, Qui-Gon Jinn"),
             ("R2-D2",            "C-3PO"),
             ("C-3PO",            "R2-D2"),
             ("Darth Maul",       "Sheev Palpatine"),
             ("Chewbacca",        "Han Solo"),
             ("Lando Calrissian", "Han Solo"),
             ("Jabba",            "Boba Fett")
            ).toDF("name", "friends")
     namesDF.show() 
     namesDF.printSchema()
     import org.apache.spark.sql.functions._
     namesDF.filter($"friends" === "Han Solo").show()
  }
}