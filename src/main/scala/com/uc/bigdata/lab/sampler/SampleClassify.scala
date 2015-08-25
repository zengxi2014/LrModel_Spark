package com.uc.bigdata.lab.sampler

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object SampleClassify {

  //广播变量
  private var featIdMapBc: Broadcast[Map[String, Long]] = null

  def loadData(iter: Iterator[String]): Iterator[(String, String)] = {
    var res = List[(String, String)]()
    while (iter.hasNext) {
      val line = iter.next()
      var url = "";
      var tit = ""
      val fields = line.split("`")
      for (i <- 0 to fields.length - 1) {
        if (fields(i).startsWith("url=")) {
          url = fields(i)
        }
        if (fields(i).startsWith("tit=")) {
          tit = fields(i)
        }
      }
      var matchDomain = ""
      var flag = false
      for (i <- 0 to NotPronDomains.othersDomains.length - 1) {
        if (url.indexOf(NotPronDomains.othersDomains(i).substring(0,
          NotPronDomains.othersDomains(i).indexOf("``"))) != -1) {
          flag = true;
          if (matchDomain.equals(""))
            matchDomain = NotPronDomains.othersDomains(i)
          else {
            val curDomain = NotPronDomains.othersDomains(i)
            if (curDomain.split("\\.").length > matchDomain.split("\\.").length)
              matchDomain = curDomain
            else if (curDomain.split("\\.").length == matchDomain.split("\\.").length) {
              if (curDomain.length() > matchDomain.length()) {
                matchDomain = curDomain
              }
            }
          }
        }
      }
      for (i <- 0 to PronDomains.pronDomains.length - 1) {
        if (url.indexOf(PronDomains.pronDomains(i).substring(0,
          PronDomains.pronDomains(i).indexOf("``"))) != -1) {
          flag = true;
          if (matchDomain.equals(""))
            matchDomain = PronDomains.pronDomains(i)
          else {
            val curDomain = PronDomains.pronDomains(i)
            if (curDomain.split("\\.").length > matchDomain.split("\\.").length)
              matchDomain = curDomain
            else if (curDomain.split("\\.").length == matchDomain.split("\\.").length) {
              if (curDomain.length() > matchDomain.length()) {
                matchDomain = curDomain
              }
            }
          }
        }
      }
      for (i <- 0 to PronDomains2.pronDomains.length - 1) {
        if (url.indexOf(PronDomains2.pronDomains(i).substring(0,
          PronDomains2.pronDomains(i).indexOf("``"))) != -1) {
          flag = true;
          if (matchDomain.equals(""))
            matchDomain = PronDomains2.pronDomains(i)
          else {
            val curDomain = PronDomains2.pronDomains(i)
            if (curDomain.split("\\.").length > matchDomain.split("\\.").length)
              matchDomain = curDomain
            else if (curDomain.split("\\.").length == matchDomain.split("\\.").length) {
              if (curDomain.length() > matchDomain.length()) {
                matchDomain = curDomain
              }
            }
          }
        }
      }
      for (i <- 0 to PronDomains3.pronDomains.length - 1) {
        if (url.indexOf(PronDomains3.pronDomains(i).substring(0,
          PronDomains3.pronDomains(i).indexOf("``"))) != -1) {
          flag = true;
          if (matchDomain.equals(""))
            matchDomain = PronDomains3.pronDomains(i)
          else {
            val curDomain = PronDomains3.pronDomains(i)
            if (curDomain.split("\\.").length > matchDomain.split("\\.").length)
              matchDomain = curDomain
            else if (curDomain.split("\\.").length == matchDomain.split("\\.").length) {
              if (curDomain.length() > matchDomain.length()) {
                matchDomain = curDomain
              }
            }
          }
        }
      }
      if (flag == true) {
        val resline = "category=" + matchDomain.substring(matchDomain.indexOf("``") + 2) + "`" + line
        res.::=(tit, resline)
      }
      // 格式：tit,line
    }
    res.iterator
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().set("spark.akka.frameSize", "1024")
    val sc = new SparkContext(conf)
    val data = sc.textFile("/user/uaewa/updc/sys_data/browse_data_hour/2015/06/20/*/part-r-*", 616)
    val sample = data.sample(false, 0.1, 0).mapPartitions(loadData)
    val mapresult = sample.saveAsTextFile("/user/zengmx/Sample_Spark_0620_0729_mapresult")
    val result = sample.sample(false, 0.1, 0).reduceByKey((a, b) => a).map(_._2)
    result.saveAsTextFile("/user/zengmx/Sample_Spark_0620_0729_reduceresult")
    sc.stop()
  }
}