package com.test

import scala.io.Source

object OrderRevenue {
  def main(args: Array[String]): Unit = {
    println("Inside Main")
    val orderItems = Source.fromFile("C:\\murali\\data-master\\retail_db\\order_items\\part-00000").getLines
    val filteredOrders = orderItems.filter(orderItem => orderItem.split(",")(1).toInt == 2)
    val filteredOrdersMap = filteredOrders.map(orderItem => orderItem.split(",")(4).toFloat)
    println("Revenue:"+filteredOrdersMap.reduce((total,orderItemSubTotal) => total + orderItemSubTotal))
    //val filteredOrders = orderItems.filter(_.split(",")(1).toInt == 2)
    //filteredOrdersMap.reduce(_+_)
  }
}