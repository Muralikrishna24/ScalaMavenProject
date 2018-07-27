package com.test

object CollectionsExamples {
  def main(args: Array[String]): Unit = {
    val studentName: List[String] = List("Murali","Raghava","Sudheer")
    studentName.foreach(println)
    val twoDimList = List(List("Int","varchar"),List("integer","float"))
    twoDimList.foreach(println)
  }
} 