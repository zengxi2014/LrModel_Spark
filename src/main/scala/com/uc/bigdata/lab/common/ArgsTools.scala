package com.uc.bigdata.lab.common

import org.apache.spark.SparkContext
import com.typesafe.config.ConfigFactory
import scala.collection.mutable
import com.typesafe.config.ConfigObject
import java.util.AbstractMap.SimpleImmutableEntry
import com.typesafe.config.Config
import scala.Array.canBuildFrom
import scala.collection.JavaConverters._
import org.apache.spark.Logging
import org.apache.hadoop.fs.FileSystem

object ArgsTools extends Logging {

  case class InputLogs(inputSource: String, path: String)

  case class Parameters(
    domainsDir: String, urlPartternFile: String,
    keyWordsFile: String, stopWordsFile: String,
    pageTagNum: Int, userTagNum: Int,
    conf: com.typesafe.config.Config,
    otherArgs: Map[String, String])

  /** 把传入的命令行参数解释为Map **/
  def parserArgs(args: Array[String]): Map[String, String] = {
    args.map(item => {
      val kv = item.split("=", 2).map(_.trim) // 去除额外的空格
      val k = kv(0).stripPrefix("-").stripPrefix("-") //兼容 -key= 和 --key=的情况
      if (kv.length >= 2) {
        k -> kv(1)
      } else {
        (k, "")
      }
    }).toMap
  }

  /** 通过命令参数的key获取对应的值， 如没有找到抛出异常  **/
  private def getArgsValue(argsMap: Map[String, String], key: String): String = {
    argsMap.get(key) match {
      case Some(value) =>
        if (value.trim.isEmpty) {
          throw new Exception(s"--${key} didn't provided")
        } else {
          value.trim
        }
      case None => throw new Exception(s"--${key} didn't provided")
    }
  }

  /** 解释输入日志参数，如果有多个目录以`隔开，处理他们为InputLogs对象，方便使用  **/
  private def handlLogArgs(logsInput: String): mutable.ArrayBuffer[InputLogs] = {
    val result = mutable.ArrayBuffer[InputLogs]()
    logsInput.split("`").foreach(log => {
      val kv = log.split("=", 2).map(_.trim)
      result += InputLogs(kv(0), kv(1))
    })
    result
  }

  /** 从HDFS上读取配置文件，并使用typesaft的config库，对配置文件进行解释，返回配置对象 **/
  def parserConfig(sc: SparkContext, confPath: String): Config = parserConfig(confPath)

  def parserConfig(confPath: String, readFunc: String => String = Utils.readHadoopSmallFile): Config = {
    ConfigFactory.parseString(readFunc(confPath))
  }

  /**
   * 解释输入参数
   */
  def parserArgs(context: SparkContext, args: Array[String]): Parameters = {
    val parsedArgs = parserArgs(args)

    //val logsInput = getArgsValue(parsedArgs, "logs")
    //val output = getArgsValue(parsedArgs, "output")
    val confPath: String = getArgsValue(parsedArgs, "confPath")

    // 加载配置文件, 因为是配置文件，所以可以不检查是否有值，配置文件会给默认值
    val conf: Config = parserConfig(context, confPath)

    // 解释输入日志, 按输入源分类，以便按源处理
    //val logInputs = handlLogArgs(logsInput)

    val otherArgs = parsedArgs -- List("confPath")

    Parameters(
      domainsDir = conf.getString("metaData.domainsDir"),
      urlPartternFile = conf.getString("metaData.urlPartternFile"),
      keyWordsFile = conf.getString("metaData.keyWordsFile"),
      stopWordsFile = conf.getString("metaData.stopWordsFile"),
      pageTagNum = conf.getInt("tag.pageTag"),
      userTagNum = conf.getInt("tag.userTag"),
      conf,
      otherArgs)
  }

}