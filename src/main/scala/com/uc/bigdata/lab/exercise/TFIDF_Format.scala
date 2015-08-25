package com.uc.bigdata.lab.common

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Logging
import scala.collection.mutable.HashMap
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils

object TFIDF_Format {

  //广播变量
  private var featIdMapBc: Broadcast[Map[String, Long]] = null
  private var IdfMapBc: Broadcast[Map[Long, Double]] = null
  /**
   * 抽取标题的关键字
   */
  def extractTit(line: String): String = {
    val fields = line.split(("`"))
    var tit = ""
    for (i <- 0 to fields.length - 1) {
      if (fields(i).startsWith("tit="))
        tit = fields(i)
    }
    // val tit = line.split(("`"))(2)
    val word = tit.substring(4).replace("'", "").split(" ")
    var res = ""
    for (i <- 0 to word.length - 1) {
      if (!word(i).trim().equals("")) res += word(i) + "_1 "
    }
    res = res.trim()
    res
  }
  /**
   * 抽取关键字
   */
  def extractKW(line: String): String = {
    val fields = line.split(("`"))
    var tit = ""
    for (i <- 0 to fields.length - 1) {
      if (fields(i).startsWith("kw="))
        tit = fields(i)
    }
    val word = tit.substring(3).replace("'", "").split(" ")
    var res = ""
    for (i <- 0 to word.length - 1) {
      if (!word(i).trim().equals("")) res += word(i) + "_2 "
    }
    res = res.trim()
    res
  }
  /**
   * 抽取类别
   */
  def extractCate(line: String): String = {
    val index = line.indexOf("category=")
    val cate = line.substring(index + 9, index + 13)
    cate
  }

  def extract(line: String): String = {
    if (extractKW(line).trim().equals("") && extractTit(line).trim().equals("")) {
      extractCate(line)
    } else if (extractTit(line).trim().equals("")) {
      extractCate(line) + " " + extractKW(line)
    } else if (extractKW(line).trim().equals("")) {
      extractCate(line) + " " + extractTit(line)
    } else {
      extractCate(line) + " " + extractTit(line) + " " + extractKW(line)
    }
  }

  def trimDup(line: String): String = {
    val vs = line.split(" ").map(_.trim)
    var cate = vs(0)
    val kvPairs = vs.slice(1, vs.size).toSet.toArray.mkString(" ")
    "%s %s".format(cate, kvPairs)
  }

  def readFeatureSet(sc: SparkContext, data: RDD[String]): Map[String, Long] = {
    val featIdMap = data.filter(_.split("\t").length == 2).map(line => {
      val vs = line.split("\t")
      (vs(0), vs(1).toLong)
    }).toArray.toMap
    featIdMapBc = sc.broadcast(featIdMap)
    featIdMap
  }

  def readIdfMap(sc: SparkContext, data: RDD[String]): Map[Long, Double] = {
    val IdfMap = data.map(line => {
      val vs = line.split("\t")
      (vs(0).toLong, vs(1).toDouble)
    }).toArray.toMap
    IdfMapBc = sc.broadcast(IdfMap)
    IdfMap
  }

  // 格式化为libsvm标准格式并输出
  def formatSample(line: String, featIdConv: Map[String, Long], IdfConv: Map[Long, Double]): String = {
    val vs = line.split(" ").map(_.trim)
    var cate = vs(0).toInt

    val kvPairs = vs.slice(1, vs.size).filter(feat => {
      featIdConv.contains(feat)
    }).map(x => {
      val fid = featIdConv(x)
      (fid, 1.0 / (vs.size - 1) * IdfConv(fid - 1))
    }).toSet.toArray.sortWith((e1, e2) => {
      e1._1 - e2._1 < 0
    }).map(x => {
      val (fid, fval) = (x._1, x._2)
      "%d:%f".format(fid, fval)
    }).mkString(" ")
    "%d %s".format(cate, kvPairs)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val feaId = sc.textFile("/user/zengmx/FeatureOutput_0731/part-r-00000")
    readFeatureSet(sc, feaId)
    val idf_file = sc.textFile("/user/zengmx/0731/idf.model")
    readIdfMap(sc, idf_file)
    val featIdMapConv = featIdMapBc.value.toMap
    val IdfMapConv = IdfMapBc.value.toMap
    val sample = sc.textFile("/user/zengmx/0730/0730_unformat_test_data").map(line => extract(line)).filter(_.split(" ").length != 1).map(line => formatSample(line, featIdMapConv, IdfMapConv)).filter(_.split(" ").length != 1)
    sample.saveAsTextFile("/user/zengmx/FormatSample_0730_tfidf_test")
    sc.stop()
  }
}