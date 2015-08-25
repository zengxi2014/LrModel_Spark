package com.uc.bigdata.xss

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.feature.ChiSqSelectorModel
import com.uc.bigdata.lab.common.ArgsTools
import com.uc.bigdata.lab.common.Utils
import org.apache.spark.mllib.feature.ChiSqSelectorModel
import org.apache.spark.SparkConf
import spire.math.UInt
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.classification.NaiveBayes


/**
 * 模型训练
 * 
 * 输入：
 *  -<raw_inst>: 原始样本，类目标签 \s 特征:特征值 \s 特征:特征值 \s ...
 * 输出：
 *  -<feat_id_map>：一级特征ID映射表，格式：特征 \t 特征ID
 *  -<feat_sel_id_map>：二级特征ID映射表，即特征选择模型，格式：特征ID \t 选择后的特征ID
 *  -<classify_model>：分类模型，包括mllib标准格式和自定义格式。自定义格式：
 *    * 随机森林：暂无
 *    * 朴素贝叶斯：
 * 处理：
 * 1. 样本格式化为libsvm格式
 * 2. 特征选择
 * 3. 训练分类器
 * 
 */
object Train extends Logging {
  
  private val conf = new SparkConf
  private var sc: SparkContext = null
  
  private var featIdMapBc: Broadcast[Map[String, Long]] = null
  private var numFeats = 100
  
  // 模型参数配置
  private val DEFAULT_NUM_FEATS = 100
  private val DEFAULT_NUM_TREES = 100
  private val DEFAULT_TREE_MAX_DEPTH = 4
  private val DEFAULT_TREE_MAX_BINS = 32
  private val DEFAULT_TREE_IMPURITY = "gini"
  private val DEFAULT_SMOOTH_NB = 0.1
  
  
  /**
   * 特征样本转储为标准libsvm格式
   * 输出：
   *  - 特征ID映射表：fid \t feature
   *  - libsvm格式样本
   * 
   * @param data 过滤后的原始样本
   * @param cateIdMapBc 共享类目ID映射表
   * @param featIdMapping 特征ID映射表的输出路径
   * @param urlInstFm libsvm格式化样本的输出路径
   */
  def formatInstLibsvm(data: RDD[String], cateIdMapBc: Broadcast[Map[String, Int]], 
      featIdMapping: String, urlInstFm: String): Int = {
    
    // 生成特征ID映射表：feature -> fid
    val featIdMap = data.flatMap(line => {
      val vs = line.split(" ").map(_.trim)
      vs.slice(1, vs.size).map(elem => {
        val kv = elem.split(":").map(_.trim)
        val feat = kv(0)
        (feat, 1)
      })
    }).reduceByKey(
      (a, b) => (a+b)
    ).filter(_._2>30).map(_._1).zipWithIndex().map(  // 特征ID从1开始
      x => (x._1, x._2+1)
    ).toArray.toMap
    
    featIdMapBc = sc.broadcast(featIdMap)
    
    // 格式化输出特征ID映射表：fid \t feature 
    val featIdMapBuf = new ArrayBuffer[String]()
    val featIdMapSortArr = featIdMap.toArray.sortWith((e1,e2) => {
      e1._2-e2._2<0
    })
    for (i <- 0 until featIdMapSortArr.length) {
      val feat = featIdMapSortArr(i)._1
      val fid = featIdMapSortArr(i)._2
      featIdMapBuf += "%d\t%s".format(fid, feat)
    }
    val outFeatIdMap = featIdMapBuf.mkString("\n")
    sc.parallelize(Array(outFeatIdMap), 1).coalesce(1).saveAsTextFile(featIdMapping)
    
    // 格式化为libsvm标准格式并输出
    val featIdMapConv = featIdMapBc.value.toMap
    data.map(line => {
      val vs = line.split(" ").map(_.trim)
      val cate = vs(0)
      val cateId = cateIdMapBc.value(cate)
      val kvPairs = vs.slice(1, vs.size).filter(x => {
        val kv = x.split(":").map(_.trim)
        val (feat, fval) = (kv(0), kv(1).toInt)
        featIdMapConv.contains(feat)
      }).map(x => {
        val kv = x.split(":").map(_.trim)
        val (feat, fval) = (kv(0), kv(1).toInt)
        val fid = featIdMapConv(feat)
        (fid, fval)
      }).sortWith((e1,e2) => {
        e1._1-e2._1 < 0
      }).map(x => {
        val (fid, fval) = (x._1, x._2)
        "%d:%d".format(fid, fval)
      }).mkString(" ")
      "%d %s".format(cateId, kvPairs)
    }).saveAsTextFile(urlInstFm)
    
    featIdMap.size
  }
  
  /**
   * 特征选择
   * 输出：
   *  - 特征白名单
   *  - 特征选择后的样本
   * note: 对比不使用特征选择的情况，先进行特征选择能够带来较大的模型提升
   * 
   * @param urlInstFm libsvm格式化的特征样本
   * @param urlInstSel 特征选择后的样本的输出路径
   * @param featSelcnt 
   * @param featSelIdMapping 特征选择模型，即二级特征ID映射表，格式：特征ID \t 选择后的特征ID
   */
  def doFeatureSelect(urlInstFm: String, urlInstSel: String, featSelcnt: Int, 
      featSelIdMapping: String): RDD[LabeledPoint] = {
    // 加载样本
    // 注：fid会自动减1，因此要求输入的libsvm的特征下标从1开始
    val data = MLUtils.loadLibSVMFile(sc, urlInstFm+"/part-*")
    
    if (featSelcnt > 0) { // 特征选择数大于0则启用特征选择
      // 卡方特征选择器
      // 注：可以根据以下参数获取特征选择结果
      // ChiSqSelectorModel::selectedFeatures：Array[Int], 选择的特征ID列表（升序排列）, 其中原始特征ID映射为下表索引作为新的特征ID
      val selector = new ChiSqSelector(featSelcnt)
      val transformer: ChiSqSelectorModel = selector.fit(data)
      val featSelData = data.map(lp => {
        LabeledPoint(lp.label, transformer.transform(lp.features))
      })
      
      // 保存特征选择模型
      val featSelModelBuf = new ArrayBuffer[String]()
      for (i <- transformer.selectedFeatures.indices) {
        val fidOrg = transformer.selectedFeatures(i)
        featSelModelBuf += "%d\t%d".format(fidOrg, i)
      }
      sc.parallelize(Array(featSelModelBuf.mkString("\n")), 1).coalesce(1).saveAsTextFile(featSelIdMapping)
      
      // 保存特征选择结果样本
      featSelData.saveAsTextFile(urlInstSel)
      
      featSelData
    }
    else {  // 不启用特征选择
      data
    }
  }
  
  /**
   * 训练随机森林分类模型
   * 
   * @param data libsvm格式化特征样本
   * @param numClasses 类别数
   * @param numTrees 成员树个数
   * @param modelRF 输出模型的路径 
   */
  def trainRandomForest(data: RDD[LabeledPoint], numClasses: Int, numTrees: Int, modelRF: String) {
    // 切分数据集，生成训练集和测试集
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    
    // 训练随机森林模型
    // 注：categoricalFeaturesInfo存放离散型特征，如果为空，则全部为连续型特征
    val categoricalFeaturesInfo = Map[Int, Int]()
    val featureSubsetStrategy = "auto"
    val impurity = DEFAULT_TREE_IMPURITY
    val maxDepth = DEFAULT_TREE_MAX_DEPTH
    val maxBins = DEFAULT_TREE_MAX_BINS
    val model = RandomForest.trainClassifier(data, numClasses, categoricalFeaturesInfo, numTrees, 
        featureSubsetStrategy, impurity, maxDepth, maxBins)
        
    // 模型测试
    val labelAndPreds = testData.filter(point => {
      point.features.asInstanceOf[SparseVector].indices.size > 0
    }).map(point => {
      val prediction = model.predict(point.features)
      (point.label, prediction)
    })
    val errCnt = labelAndPreds.filter(r => r._1 != r._2).count()
    val classCnt = labelAndPreds.count()
    val totalCnt = testData.count()
    
    val testErr = errCnt.toDouble/classCnt
    val testAcc = (classCnt-errCnt).toDouble/classCnt
    val testRec = (classCnt-errCnt).toDouble/totalCnt
    val fscore = 2.0/(1.0/testAcc+1.0/testRec)
    
    println("numTrees:%d\tnumFeats:%d\tsamples:%d\tclassify:%d\ttestErr:%f\ttestAcc:%f\ttestRec:%f\tF1:%f".format(
        numTrees, numFeats, totalCnt, classCnt, testErr, testAcc, testRec, fscore))
    
//    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble/testData.count()
//    println("numTrees:%d\ttestErr:%f".format(numTrees, testErr))
//    println("learned classification forest model:\n"+model.toDebugString)
    
    // 保存模型
    model.save(sc, modelRF)
  }
  
  /**
   * 训练朴素贝叶斯分类模型
   * 
   * @param data libsvm格式化特征样本
   * @param smooth 平滑参数
   * @param modelNB 输出模型的路径
   */
  def trainNaiveBayes(data: RDD[LabeledPoint], smooth: Double, modelNB: String) {
    // 切分数据集，生成训练集和测试集
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    
    // 训练朴素贝叶斯模型
    val model = NaiveBayes.train(data, smooth)
    
    // 模型测试
    val labelAndPreds = testData.filter(point => {
      point.features.asInstanceOf[SparseVector].indices.size > 0
    }).map(point => {
      val prediction = model.predict(point.features)
      (point.label, prediction)
    })
    val errCnt = labelAndPreds.filter(r => r._1 != r._2).count()
    val classCnt = labelAndPreds.count()
    val totalCnt = testData.count()
    
    val testErr = errCnt.toDouble/classCnt
    val testAcc = (classCnt-errCnt).toDouble/classCnt
    val testRec = (classCnt-errCnt).toDouble/totalCnt
    val fscore = 2.0/(1.0/testAcc+1.0/testRec)
    
    println("smooth:%f\tnumFeats:%d\tsamples:%d\tclassify:%d\ttestErr:%f\ttestAcc:%f\ttestRec:%f\tF1:%f".format(
        smooth, numFeats, totalCnt, classCnt, testErr, testAcc, testRec, fscore))
        
    // 保存模型
    // 注：也可以根据下述模型参数自定义序列化、反序列化方法，并load到线上应用
    // model.label：Array[Double], 类目
    // model.pi: Array[Double], 类目对数先验概率
    // model.theta：Array[Array[Double]], 类目特征条件概率p(x_i|y)
    model.save(sc, modelNB)
  }
  
  
  def main(args: Array[String]) {
    if (args.length < 3) {
      log.error("Usage Error!")
      println("Usage: FeatSel [options]")
      println("Options: ")
      println("  --code_name=<code_name>        input category-id map")
      println("  --url_inst=<url_feat>          input url instances")
      println("  --feat_id_map=<feat_id_map>    output feature id map")
      println("  --url_inst_fm=<url_inst_fm>    output libsvm formatted url instances")
      println("  --url_inst_sel=<url_feat_sel>  output feature selected url instances")
      println("  --feat_sel_id_map=<feat_sel>   output feature selected id map")
      println("  --model=<model>                config the classifier model as [rf],[nb],...")
      println("  --rf_model=<rf_model>          output random forest model")
      println("  --num_trees=<num_trees>        config the number of trees")
      println("  --num_feats=<num_feats>        config the number of features")
      println("  --nb_model=<nb_model>          output naive bayes model")
      println("  --smooth_nb=<smooth_nb>        config the smooth factor")
      return 
    }
    
    // 解析参数
    val params = ArgsTools.parserArgs(args)
    
    def getMapValue(kvMap: Map[String,String], key: String, default: String = ""): String = {
      if (kvMap.contains(key)) {
        kvMap(key)
      } 
      else {
        default
      }
    }
    
    val codeName = getMapValue(params, "code_name")
    val urlInst = getMapValue(params, "url_inst")
    val featIdMapping = getMapValue(params, "feat_id_map")
    val urlInstFm = getMapValue(params, "url_inst_fm")
    val urlInstSel = getMapValue(params, "url_inst_sel")
    val featSelIdMapping = getMapValue(params, "feat_sel_id_map")
    val model = getMapValue(params, "model")
    var useRF = false
    var useNB = false
    model.split(",").map(x => {
      if (x.equals("rf")) useRF = true
      else if (x.equals("nb")) useNB = true
    })
    val modelRF = getMapValue(params, "rf_model")
    val numTrees = getMapValue(params, "num_trees", DEFAULT_NUM_TREES.toString).toInt
    numFeats = getMapValue(params, "num_feats", DEFAULT_NUM_FEATS.toString).toInt
    val modelNB = getMapValue(params, "nb_model")
    val smoothNB = getMapValue(params, "smooth_nb", DEFAULT_SMOOTH_NB.toString).toDouble
    
    // 初始化spark上下文
    log.info("--------------------------------------------")
    log.info("初始化spark上下文")
    sc = new SparkContext(conf)
    val reduceNum = Utils.estimateNumberOfReducers(conf, urlInst, 1.0)
    log.info("设置 spark.default.parallelism = " + reduceNum)
    conf.set("spark.default.parallelism", "" + reduceNum)
    
    // 加载类目ID映射表
    log.info("--------------------------------------------")
    log.info("加载类目ID映射表")
    val cateIdMap = sc.textFile(codeName).map(line => {
      val code2Name = line.split("`").map(_.trim)
      code2Name(1) -> code2Name(0).toInt
    }).toArray.toMap
    val cateIdMapBc = sc.broadcast(cateIdMap)
    
    // 加载并按类目过滤原始特征样本
    log.info("--------------------------------------------")
    log.info("加载并按类目过滤原始特征样本")
    val data = sc.textFile(urlInst).filter(line => {
      val vs = line.split(" ").map(_.trim)
      val cate = vs(0)
      cateIdMapBc.value.contains(cate)
    })
    
    // 格式化样本为libsvm标准格式
    log.info("--------------------------------------------")
    log.info("格式化样本为libsvm标准格式")
    val totalFeatCnt = formatInstLibsvm(data, cateIdMapBc, featIdMapping, urlInstFm)
    
    // 特征选择
    log.info("--------------------------------------------")
    log.info("特征选择")
    val featSelData = doFeatureSelect(urlInstFm, urlInstSel, numFeats, featSelIdMapping)
    
    // 训练随机森林分类模型
    if (useRF) {
    	log.info("--------------------------------------------")
    	log.info("训练随机森林分类模型")
    	trainRandomForest(featSelData, cateIdMapBc.value.values.max+1, numTrees, modelRF)
    }

    // 训练多项式朴素贝叶斯分类模型
    if (useNB) {
    	log.info("--------------------------------------------")
    	log.info("训练多项式朴素贝叶斯分类模型")
    	trainNaiveBayes(featSelData, smoothNB, modelNB)
    }
  }
}