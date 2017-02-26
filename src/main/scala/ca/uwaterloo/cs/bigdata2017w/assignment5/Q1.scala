package ca.uwaterloo.cs.bigdata2017w.assignment5

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

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    // looking at lineitem.tbl file
    var textFile = sc.textFile(args.input() + "/lineitem.tbl")
    if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemDF = sparkSession.read.parquet("/shared/cs489/data/TPC-H-0.1-PARQUET/lineitem")
      textFile = lineitemDF.rdd.map(x=>x.toString())
    }
    val shipdate = args.date()

    val counts = textFile
    .flatMap(line => {
      val lines = line.split("\\|")
      // L_SHIPDATE is at index 10 according to TPC-H benchmark
      if (lines(10).contains(shipdate)) {
        List(lines(10))
      } else {
        List()
      }
    })
    println("ANSWER=" + counts.count())
  }
}
