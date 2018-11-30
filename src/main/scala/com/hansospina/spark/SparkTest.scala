package com.hansospina.spark

import org.apache.spark.SparkContext

object SparkTest extends App {
  val sc = new SparkContext("local[*]", "RatingsCounter")
  val lines = sc.textFile("../1800.csv")
  val wrapp = lines.map(i => {
    val line = i.toString().split(",")
    (line(0), line(2), line(3).toFloat)
  })
  val prcp = wrapp.filter(i => i._2 == "PRCP")
  val prcpTemperature = prcp.map(i => (i._1, i._3))
  val minPrcp = prcpTemperature.reduceByKey((x, y) => Math.max(x, y))
  minPrcp.foreach(println)
  sc.stop()
}
