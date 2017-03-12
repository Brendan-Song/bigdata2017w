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

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args.input())
    val outputPath = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputPath, true)

    val weights = sc.textFile(args.model() + "/part-00000")
    .map(line => {
      val lines = line.split(",")
      val feature = lines(0).drop(1).toInt
      val weight = lines(1).dropRight(1).toDouble
      (feature, weight)
    })
    .collectAsMap()

    val w = sc.broadcast(weights)

    def spamminess(features: Array[Int]): Double = {
      var score = 0d
      features.foreach(f => if(w.value.contains(f)) score += w.value.get(f).get)
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
