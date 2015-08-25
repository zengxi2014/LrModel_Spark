package com.uc.bigdata.lab.common

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Logging

object WordCount extends Logging {
  def main(args: Array[String]) {
    if (args.length != 3) {
      println("usage: WordCount  <master> <input> <output>")
      return
    }
    
    log.info("Begin word count.");
    
    val params = ArgsTools.parserArgs(args);
    val input = params("input")
    val output = params("output")
    
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    
    val logs = sc.textFile(input).flatMap(line => line.split("\\s+")).map(word => (word, 1))
    var result = logs.reduceByKey((a,b) => a+b)
    result.saveAsTextFile(output)
    
    log.info("End word count");
    
  }
}