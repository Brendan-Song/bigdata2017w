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
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import tl.lin.data.pair.PairOfStrings

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)

    if (args.text()) {
      // lineitem.tbl
      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val suppliers = sc.textFile(args.input() + "/supplier.tbl")
      val parts = sc.textFile(args.input() + "/part.tbl")
      val shipdate = args.date()

      val p = parts
      .flatMap(line => {
        val lines = line.split("\\|")
        // emit p_partkey, p_name
        List(new Pair(lines(0), lines(1)))
      })

      val s = suppliers
      .flatMap(line => {
        val lines = line.split("\\|")
        // emit s_suppkey, s_name
        List(new Pair(lines(0), lines(1)))
      })

      val pBroadcast = sc.broadcast(p.collectAsMap())
      val sBroadcast = sc.broadcast(s.collectAsMap())

      val lineitems = lineitem
      .flatMap(line => {
        val lines = line.split("\\|")
        if(lines(10).contains(shipdate) && // check for date
           pBroadcast.value.get(lines(1)) != null && // check parts table
           sBroadcast.value.get(lines(2)) != null) { // check suppliers table
          List((Integer.parseInt(lines(0)), pBroadcast.value.get(lines(1)), sBroadcast.value.get(lines(2))))
        } else {
          List()
        }
      })
      .sortBy(_._1)

      val answer = lineitems.take(20)
      for(data <- answer) {
        println("(" + data._1 + "," + data._2.mkString(" ") + "," + data._3.mkString(" ") + ")")
      }
    } else {
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
      val supplierRDD = supplierDF.rdd
      val partDF = sparkSession.read.parquet(args.input() + "/part")
      val partRDD = partDF.rdd
      val shipdate = args.date()

      val p = partRDD
      .flatMap(lines => {
        // emit p_partkey, p_name
        List(new Pair(lines(0), lines(1)))
      })

      val s = supplierRDD
      .flatMap(lines => {
        // emit s_suppkey, s_name
        List(new Pair(lines(0), lines(1)))
      })

      val pBroadcast = sc.broadcast(p.collectAsMap())
      val sBroadcast = sc.broadcast(s.collectAsMap())

      val lineitems = lineitemRDD
      .flatMap(lines => {
        if(lines(10).toString.contains(shipdate) && // check for date
           pBroadcast.value.get(lines(1)) != null && // check parts table
           sBroadcast.value.get(lines(2)) != null) { // check suppliers table
          List((lines(0).toString.toInt, pBroadcast.value.get(lines(1)), sBroadcast.value.get(lines(2))))
        } else {
          List()
        }
      })
      .sortBy(_._1)

      val answer = lineitems.take(20)
      for(data <- answer) {
        println("(" + data._1 + "," + data._2.mkString(" ") + "," + data._3.mkString(" ") + ")")
      }
    }
  }
}
