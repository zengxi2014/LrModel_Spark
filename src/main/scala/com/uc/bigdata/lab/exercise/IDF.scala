
package com.uc.bigdata.lab.exercise


import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.immutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.ChiSqSelectorModel
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors
import spire.math.UInt
import java.io.BufferedWriter
import java.io.FileWriter



object IDF extends Logging {

    private val conf = new SparkConf
    private var sc: SparkContext = null

    def main(args: Array[String]): Unit = {
       /* if (args.length != 2) {
            println("usage: <inFile> <outFile>")
            return
        }*/
        sc = new SparkContext(conf)

        val inFile ="/user/zengmx/0731/0731_format_train_data"
        val outFile = "/user/zengmx/IdfModel"

        val data = MLUtils.loadLibSVMFile(sc, inFile)
        val n = data.count.toDouble

        val idfs = data.mapPartitions { iter =>
            var featCountMap = new HashMap[Int, Double]

            while (iter.hasNext) {
                val point = iter.next
                val featArray = point.features.asInstanceOf[SparseVector].indices

                for (i <- 0 until featArray.length) {
                    val feat = featArray(i)
                    val now = featCountMap.getOrElseUpdate(feat, 0.0)
                    featCountMap(feat) = now + 1.0
                }
            }
            featCountMap.iterator
        }.reduceByKey((count1, count2) => count1 + count2).map { pair =>
            pair._1 -> math.log(n / pair._2)
        }.saveAsTextFile(outFile)
    }

}