package ca.uwaterloo.cs.bigdata2017w.assignment6

import io.bespin.scala.util.Tokenizer
import io.bespin.scala.util.WritableConversions

import scala.collection.JavaConverters._
import scala.collection.mutable._
import scala.math._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.apache.log4j._
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import tl.lin.data.pair.PairOfStrings
import org.apache.spark.sql.SparkSession

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new EnsembleConf(argv)

    log.info("Input: " + args.input())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args.input())
    val outputPath = new Path(args.output())
    val method = args.method()
    FileSystem.get(sc.hadoopConfiguration).delete(outputPath, true)

    val xWeights = sc.textFile(args.model() + "/part-00000")
    .map(line => {
      val lines = line.split(",")
      val feature = lines(0).drop(1).toInt
      val weight = lines(1).dropRight(1).toDouble
      (feature, weight)
    })
    .collectAsMap()

    val yWeights = sc.textFile(args.model() + "/part-00001")
    .map(line => {
      val lines = line.split(",")
      val feature = lines(0).drop(1).toInt
      val weight = lines(1).dropRight(1).toDouble
      (feature, weight)
    })
    .collectAsMap()

    val bWeights = sc.textFile(args.model() + "/part-00002")
    .map(line => {
      val lines = line.split(",")
      val feature = lines(0).drop(1).toInt
      val weight = lines(1).dropRight(1).toDouble
      (feature, weight)
    })
    .collectAsMap()
   
    val x = sc.broadcast(xWeights)
    val y = sc.broadcast(yWeights)
    val b = sc.broadcast(bWeights)

    def spamminess(features: Array[Int]): Double = {
      var score = 0d
      if(method == "average") {
        var count = 0
        features.foreach(f => {
          if(x.value.contains(f)) {
            score += x.value.get(f).get
            count += 1
          }
          if(y.value.contains(f)) {
            score += y.value.get(f).get
            count += 1
          }
          if(b.value.contains(f)) {
            score += b.value.get(f).get
            count += 1
          }
        })
        score = score / count
      } else {
        features.foreach(f => {
          if(x.value.contains(f) && x.value.get(f).get > 0) score += 1 else score -= 1
          if(y.value.contains(f) && y.value.get(f).get > 0) score += 1 else score -= 1
          if(b.value.contains(f) && b.value.get(f).get > 0) score += 1 else score -= 1
        })
      }
      score
    }

    val test = textFile.map(line => {
      // parse input
      val lines = line.split(" ")
      val score = spamminess(lines.drop(2).map(data => data.toInt))
      (lines(0), lines(1), score, if(score > 0) "spam" else "ham")
    })

    test.saveAsTextFile(args.output())
  }
}
