package org.yolin.sparkWordCount
import org.apache.spark.{SparkConf,SparkContext}



object wordCount {

  def main(args: Array[String]): Unit = {
    val conf= new SparkConf()
    conf.setAppName("first Spark App")
    val sc: SparkContext = new SparkContext(conf)
    val lines = sc.textFile("hdfs://127.0.0.1:9000/input/core-site.xml")
    val words = lines.flatMap {line => line.split(" ") }
    val pairs = words.map { word => (word,1) }
    val wordCounts = pairs.reduceByKey(_+_)
      .map(pair => (pair._2,pair._1))
      .sortByKey(false).map(pair => (pair._2,pair._1))

    wordCounts.collect.foreach(wordNumberPair => println(wordNumberPair._1 +":" + wordNumberPair._2))
    sc.stop
  }
}

