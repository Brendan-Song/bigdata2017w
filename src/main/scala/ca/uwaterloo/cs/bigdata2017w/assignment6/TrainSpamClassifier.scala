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

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args.input())
    val outputPath = new Path(args.model())
    val shuffle = args.shuffle()
    FileSystem.get(sc.hadoopConfiguration).delete(outputPath, true)

    // w is the weight vector (make sure the variable is within scope)
    val w = Map[Int, Double]()

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    if(!shuffle) {
      val trained = textFile.map(line => {
        // parse input
        val lines = line.split(" ")
        (0, (lines(0), lines(1) == "spam", lines.drop(2).map(data => data.toInt)))
      })
      .groupByKey(1)
      .map(data => {
        // do learning here
        // This is the main learner:
        val delta = 0.002

        // For each instance...
        data._2.foreach(instance => {
          val isSpam = if(instance._2) 1 else 0   // label
          val features = instance._3 // feature vector of the training instance

          // Update the weights as follows:
          val score = spamminess(features)
          val prob = 1.0 / (1 + exp(-score))
          features.foreach(f => {
            if (w.contains(f)) {
              w(f) += (isSpam - prob) * delta
            } else {
              w(f) = (isSpam - prob) * delta
            }
          })
        })
        w
      })
      .flatMap(data => data)
      trained.saveAsTextFile(args.model())
    } else {
      val trained = textFile.map(line => {
        // parse input
        val lines = line.split(" ")
        (0, (lines(0), lines(1) == "spam", lines.drop(2).map(data => data.toInt), scala.util.Random.nextInt()))
      })
      .sortBy(data => data._2._4)
      .groupByKey(1)
      .map(data => {
        // do learning here
        // This is the main learner:
        val delta = 0.002

        // For each instance...
        data._2.foreach(instance => {
          val isSpam = if(instance._2) 1 else 0   // label
          val features = instance._3 // feature vector of the training instance

          // Update the weights as follows:
          val score = spamminess(features)
          val prob = 1.0 / (1 + exp(-score))
          features.foreach(f => {
            if (w.contains(f)) {
              w(f) += (isSpam - prob) * delta
            } else {
              w(f) = (isSpam - prob) * delta
            }
          })
        })
        w
      })
      .flatMap(data => data)
      trained.saveAsTextFile(args.model())
    }
  }
}
