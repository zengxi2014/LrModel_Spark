package com.uc.bigdata.lab.common

import org.slf4j.LoggerFactory
import scala.io.BufferedSource
import scala.io.Source
import java.io.File
import com.uc.bigdata.lab.common._
import java.util.Calendar
import org.apache.spark.{ Logging, SparkContext, SparkConf }
import org.apache.spark.SparkContext._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.ContentSummary
import org.apache.hadoop.fs.Path
import java.io.IOException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import java.io.OutputStreamWriter
import java.io.BufferedWriter
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FileUtil

case class DomainPath(domain: String, path: String)
case class Domain(p0: String, p1: String, p2: String)

object Utils extends Logging {

  def readFile(path: String): BufferedSource = Source.fromFile(path, "UTF-8")
  def readFile(f: File): BufferedSource = Source.fromFile(f, "UTF-8")

  def readHadoopFile(path: String): BufferedSource = {
    val fs = Utils.getFS
    val is = fs.open(new Path(path))
    Source.fromInputStream(is, "UTF-8")
  }

  def readHadoopSmallFile(path: String): String = {
    val fs = Utils.getFS
    val is = fs.open(new Path(path))
    val lines = Source.fromInputStream(is, "UTF-8").mkString
    is.close()
    lines
  }

  def readHadoopSmallFileToLines(path: String): Seq[String] = {
    val fs = Utils.getFS
    val is = fs.open(new Path(path))
    val lines = Source.fromInputStream(is, "UTF-8").getLines.toList
    is.close()
    lines
  }

  // 毫秒级的Ts
  def takeTsByDay(day: Int, ts: Long = System.currentTimeMillis()): Long = {
    val c = Calendar.getInstance()
    c.setTimeInMillis(ts)
    c.add(Calendar.DAY_OF_MONTH, -day)
    c.set(Calendar.HOUR_OF_DAY, 0)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.SECOND, 0)
    c.set(Calendar.MILLISECOND, 0)
    c.getTimeInMillis()
  }

  /**
   * Log中value可以为空，但key不能为空
   */
//  def getRecord(log: String): LogDataRecord = CaseClassUtils.getRecord(log)
//
//  def parserRecorde(line: String): Map[String, String] = {
//    line.split(Constants.FIELD_SEP)
//      .map(_.trim)
//      .filter(!_.isEmpty)
//      .map(field => {
//        val kv = field.split(Constants.KV_SEP, 2)
//        if (kv.length >= 2) {
//          kv(0) -> kv(1)
//        } else {
//          (kv(0), "")
//        }
//      }).toMap
//  }

  /**
   * Log中value可以为空，但key不能为空
   */
  def getFieldValue(log: String, field: String): String = {
    val f = s"${field}="
    val s = log.indexOf(f)
    if (s < 0) return ""
    val e = {
      val e = log.indexOf("`", s + f.length)
      if (e >= 0) e else log.length
    }
    log.substring(s + f.length(), e)
  }

  def getFS() = FileSystem.get(SparkHadoopUtil.get.newConfiguration())

  /**
   * return  uid && imei && snid || func(sn)""
   */
  def getUID(log: String): String = {
    getUID(name => getFieldValue(log, name))
  }

  def getUID(map: Map[String, String]): String = {
    getUID(name => map.getOrElse(name, ""))
  }

  // return a-b-c  || ""
  def sn2snid(sn: String): String = {
    val ss = sn.split("-")
    if (ss.size != 3) return ""
    val snid = ss(1)
    snid
  }

  private def getUID(func: String => String): String = {
    Array("uid", "imei", "snid").foreach { name =>
      val value = func(name).trim
      if (!value.isEmpty && value.toLowerCase != "null") {
        return value
      }
    }
    val sn = func("sn").trim
    if (!sn.isEmpty) {
      sn2snid(sn)
    } else {
      ""
    }
  }

//  def getLogParts(logRecorde: LogDataRecord): String = {
//    s"url=${logRecorde.url}`ori_text=${logRecorde.orgText}`tit=${logRecorde.title}`kw=${logRecorde.kw}`uid=${logRecorde.uid}"
//  }

  def trimUrl(url: String): String = {
    Seq("http://", "https://", "www.", "wap.", "wap2.", "wap3.",
      "iphone.", "phone.", "3g.", "i.", "m.", "g.", "touch.", "webapp.")
      .foldLeft(url.toLowerCase)((url, str) => {
        url.stripPrefix(str)
      })
  }

  // 从url中提取域名和第一级path
  private def getDomainPath(url_ : String): DomainPath = {
    try {
      if (url_.isEmpty) {
        return DomainPath("", "")
      }

      // 从url中获取域名， 同时去除http:// 及www. wap.
      val url = trimUrl(url_)

      //(域名，一级path)
      val domainPaths = url.split("/", 3)

      val domain = if (domainPaths(0).contains("?")) domainPaths(0).split("\\?", 2)(0) else domainPaths(0)

      val path = domainPaths.length match {
        case 1 => "" //没有path
        case 2 => //处理 search?q=mllib&oq=mllib 和home#/client/userattrcoverage 这两种情况
          val paths = domainPaths(1).split("\\?")
          if (paths.length > 0) {
            if (paths(0).split("#").length > 0) paths(0).split("#")(0) else ""
          } else {
            ""
          }
        case _ => if (domainPaths(1).split("#").length > 0) domainPaths(1).split("#")(0) else ""
      }

      val _path = if (path.startsWith("?")) "" else path

      DomainPath(domain.trim, _path)
    } catch {
      case e: Exception =>
        logError("getDomainPath failed", e)
        DomainPath("", "")
    }
  }

  /**
   * 传入一个页面的url,返回域名及其二级目录,如果没有二级目录则返回以及一级目录，没有以及目录则直接返回域名
   * 例如 www.ent.sina.com/yule/tv/342335.html则返回ent.sina.com/yule/tv
   * 例如 www.ent.sina.com/yule/342335.html则返回ent.sina.com/yule
   * 例如 www.ent.sina.com/342335.html则返回ent.sina.com
   */
  def getDomainBK(urlSrc: String): String = {
    val url = trimUrl(urlSrc)
    var domain = ""
    var first = url.indexOf("/")
    if (first >= 0) {
      val second = url.indexOf("/", first + 1)
      if (second >= 0) {
        val third = url.indexOf("/", second + 1)
        if (third >= 0) {
          domain = url.substring(0, third)
        } else {
          domain = url.substring(0, second)
        }
      } else {
        domain = url.substring(0, first)
      }
    } else {
      domain = url.split("/", 3)(0)
    }
    //并对包含？的url进行处理
    if (domain.contains("?")) {
      domain = domain.split("\\?", 2)(0)
    }
    domain
  }

  def getDomain(urlSrc: String): Domain = {
    val url = trimUrl(urlSrc)
    val domain = url.split("/", 4).map(_.trim)
    val p0 = {
      if (domain.length > 0 && !domain(0).isEmpty && !domain(0).startsWith(".")) {
        domain(0)
      } else {
        ""
      }
    }
    val p1 = if (domain.length > 1) domain(1) else ""
    val p2 = if (domain.length > 2) domain(2) else ""

    Domain(p0, p1, p2)
  }

  def setCheckpointDir(context: SparkContext) {
    val fs = FileSystem.get(context.hadoopConfiguration)
    val home = fs.getHomeDirectory().toString().stripSuffix("/")

    context.setCheckpointDir(s"${home}/tmp/spark-checkpoint")
  }

  def adjustReduceNumber(reduceNum: Int, factor: Double): Int = {
    Math.max((reduceNum * factor).toInt, 1)
  }

  /**
   * 估算partition数量。以数据量估算。类似之前的自动计算reduce数量。
   * factor : 默认1g一个reduce，用这个参数调整。默认值是1.0
   */
  def estimateNumberOfReducers(sparkConf: SparkConf, inputPaths: String, factor: Double = 1.0): Int = {
    // 0.95或者1.75 ×（节点数 ×mapred.tasktracker.tasks.maximum参数值）

    val BYTES_PER_REDUCER = "uc.exec.reducers.bytes.per.reducer";
    val MAX_REDUCERS = "uc.exec.reducers.max";
    val DEF_BYTES_PER_REDUCER: Long = (1000L * 1000 * 1000 * factor).toLong;
    val DEF_MAX_REDUCERS: Int = 999;

    val conf = getHadoopConf(sparkConf);

    val bytesPerReducer = conf.getLong(BYTES_PER_REDUCER,
      DEF_BYTES_PER_REDUCER);

    val maxReducers = conf.getInt(MAX_REDUCERS, DEF_MAX_REDUCERS);

    val totalInputFileSize = getInputSummary(conf, inputPaths).getLength();

    // 按数据量计算得到的reducer数量
    var reducers = ((totalInputFileSize + bytesPerReducer - 1) / bytesPerReducer).toInt;
    reducers = Math.max(1, reducers);

    val runModel = SparkRunModel(sparkConf)

    val maxReduceTasks = runModel.model match {
      case runModel.standalone =>
        sparkConf.get("spark.cores.max", s"${maxReducers}").toInt
      case runModel.yarn =>
        val executorNum = sparkConf.get("spark.executor.instances", "5").toInt
        val executorCores = sparkConf.get("spark.executor.cores", "1").toInt
        executorNum * executorCores;
      case runModel.mesos => maxReducers
    }

    // 如果按输入数据计算得到的reducer数远大于reduce的槽数，使用1.75，否则使用0.95
    // 按系统槽数计算得到的reducer数量

    var reducersOnStatus = maxReduceTasks * 95 / 100;
    if (reducers >= maxReduceTasks * 3) { // *3 -> 远大于
      reducersOnStatus = (maxReduceTasks * 175 + 100 - 1) / 100; // 向上取整
    }
    reducersOnStatus = Math.max(1, reducersOnStatus);

    reducers = Math.min(reducersOnStatus, reducers);

    reducers = Math.min(maxReducers, reducers);

    return reducers;
  }

  /**
   * 计算Job输入文件的大小
   *
   * @param job
   *            hadoop job
   * @return 所有输入路径的汇总
   * @throws IOException
   */
  private def getInputSummary(conf: Configuration, inputPaths: String): ContentSummary = {
    val summary = Array(0L, 0L, 0L)
    val dirs = inputPaths;

    def needGlob(path: String) = {
      path.contains("*") ||
        path.contains("?") ||
        (path.contains("[") && path.contains("]"))
    }

    def addOne(p: Path) = {
      val fs = p.getFileSystem(conf);
      val cs = fs.getContentSummary(p);
      summary(0) += cs.getLength();
      summary(1) += cs.getFileCount();
      summary(2) += cs.getDirectoryCount();
    }

    // 存在可以不设置输入路径的情况（重载InputFormat）
    for (path: String <- dirs.split(",")) {
      try {
        val p = new Path(path);
        val fs = p.getFileSystem(conf);

        if (needGlob(path)) {
          val many = fs.globStatus(p)
          many.foreach(status => {
            val p = status.getPath()
            addOne(p)
          })
        } else {
          addOne(p)
        }
      } catch {
        case e: IOException =>
      }
    }

    return new ContentSummary(summary(0), summary(1), summary(2));
  }

  private def getHadoopConf(conf: SparkConf): org.apache.hadoop.conf.Configuration = {
    val hadoopConf = SparkHadoopUtil.get.newConfiguration()
    conf.getAll.foreach {
      case (key, value) =>
        if (key.startsWith("spark.hadoop.")) {
          hadoopConf.set(key.substring("spark.hadoop.".length), value)
        }
    }
    val bufferSize = conf.get("spark.buffer.size", "65536")
    hadoopConf.set("io.file.buffer.size", bufferSize)
    hadoopConf
  }

  // TODO 进入垃圾站而不直接删除
  def cleanDir(outputPath: String) = {
    val fs = getFS
    fs.delete(new Path(outputPath))
  }

//  def buildTmpOutputPath(prefix: String): String = {
//    val fs = getFS
//    val user = Utils.user
//    val homeDir = s"/user/${user}"
//    val outputPath = s"${homeDir}/tmp/tmp_${prefix}_${PathUtils.datetimeString()}"
//    Utils.cleanDir(outputPath)
//    outputPath
//  }

  /** 内连接， 做hash连接 **/
  def join(kvR: RDD[(String, String)], kR: RDD[String]): RDD[(String, String)] = {
    val kR2 = kR.map((_, None))
    kvR.join(kR2).map(e => (e._1, e._2._1))
  }

  def user(): String = {
    System.getProperty("user.name")
  }

  def workDir(): String = s"/user/${user}/waup_spark"

//  def saveTmpFile(data: Iterator[Any], limit: Int): String = {
//    var writed = 0
//    val fs = Utils.getFS
//    val user = Utils.user
//    val homeDir = s"/user/${user}"
//    val tmpPath = Utils.buildTmpOutputPath("pickout_limit")
//
//    fs.mkdirs(new Path(tmpPath))
//    val tmpFile = s"${tmpPath}/pickout_limit.list.txt"
//    val out = fs.create(new Path(tmpFile))
//    val writer = new BufferedWriter(new OutputStreamWriter(out, "utf8"))
//    for (line <- data if writed < limit) {
//      writed = writed + 1
//      writer.append(line.toString).write("\n")
//    }
//    writer.close()
//    tmpFile
//  }

  def saveSmallFile(path: Path, content: String) {
    val fs = Utils.getFS
    val out = fs.create(path)
    out.write(content.getBytes("UTF-8"))
    out.close()
  }

  case class SparkRunModel(sparkConf: SparkConf) {
    val standalone = "standalone"
    val yarn = "yarn"
    val mesos = "mesos"

    val model = sparkRunModel(sparkConf)

    private def sparkRunModel(sparkConf: SparkConf): String = {
      val MASTER = "spark.master"
      val DEFAULT_RUN_MODEL = "standalone"
      if (!sparkConf.contains(MASTER)) return DEFAULT_RUN_MODEL
      sparkConf.get("spark.master") match {
        case x if x.startsWith("spark://") => "standalone"
        case x if x.startsWith("yarn") => "yarn"
        case x if x.startsWith("mesos://") => "mesos"
        case _ => DEFAULT_RUN_MODEL
      }
    }
  }

  def touchFile(file: String) {
    val fs = Utils.getFS
    val outFile = new Path(file)
    val out = fs.create(outFile)
    out.close()
  }

  def touchSuccessFile(output: String) {
    val successFile = s"${output}/_SUCCESS"
    touchFile(successFile)
  }

  // 计算用户活跃天数
  def bitsToLive(bits: Int): Int = Integer.bitCount(bits)

  /**
   * 用于计算bitmap, 把day天数转为bitmap
   */
  def dayToBit(d: Int): Int = {
    if (d <= 0 || d > 30) {
      throw new IllegalArgumentException(s"day[${d}] can't less than 0 or great than 30")
    }
    1 << (d - 1)
  }

}
