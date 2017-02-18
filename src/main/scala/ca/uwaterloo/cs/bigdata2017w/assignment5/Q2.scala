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

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    // lineitem.tbl
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
    val orders = sc.textFile(args.input() + "/orders.tbl")
    val shipdate = args.date()

    val lineitems = lineitem
    .flatMap(line => {
      val lines = line.split("\\|")
      // l_shipdate is at index 10 according to TPC-H benchmark
      if (lines(10).contains(shipdate)) {
        // emit l_orderkey and l_shipdate
        List(new Pair(lines(0), lines(10)))
      } else {
        List()
      }
    })

    val clerks = orders
    .flatMap(line => {
      val lines = line.split("\\|")
      // emit o_orderkey and o_clerk
      List(new Pair(lines(0), lines(6)))
    })

    val grouped = clerks
    .cogroup(lineitems)
    .flatMap(data => {
      // data is formatted as: {orderkey, {{clerks}, {lineitems}}}
      val o_key = data._1
      val lineitemlist = data._2._2
      // make sure date is there
      if(!lineitemlist.isEmpty) {
        val clerklist = data._2._1
        val clerk = clerklist.map(x => x)
        List(new Pair(Integer.parseInt(o_key), clerk.mkString(" ")))
      } else {
        List()
      }
    })
    .sortByKey(true)

    val answer = grouped.take(20)
    for(pair <- answer) {
      println("(" + pair._2 + "," + pair._1 + ")")
    }
  }
}
