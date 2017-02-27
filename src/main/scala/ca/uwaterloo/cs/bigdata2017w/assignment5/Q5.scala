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

object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    if (args.text()) {
      // lineitem.tbl
      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      // orders.tbl
      val orders = sc.textFile(args.input() + "/orders.tbl")
      // customer.tbl
      val customer = sc.textFile(args.input() + "/customer.tbl")
      // nation.tbl
      val nation = sc.textFile(args.input() + "/nation.tbl")

      val n = nation
      .map(line => {
        val lines = line.split("\\|")
        // emit n_nationkey, n_name
        new Pair(lines(0), lines(1))
      })

      val nBroadcast = sc.broadcast(n.collectAsMap())

      val c = customer
      .flatMap(line => {
        val lines = line.split("\\|")
        // emit c_custkey, c_nationkey if country is Canada or United States
        if (nBroadcast.value.get(lines(3)).get != null &&
            (nBroadcast.value.get(lines(3)).get == "CANADA" ||
             nBroadcast.value.get(lines(3)).get == "UNITED STATES")) {
          List(new Pair(lines(0), lines(3)))
        } else {
          List()
        }
      })

      val cBroadcast = sc.broadcast(c.collectAsMap())

      val o = orders
      .flatMap(line => {
        val lines = line.split("\\|")
        val nationkey = cBroadcast.value.get(lines(1))
        if (nationkey != null && !nationkey.isEmpty) {
          // emit {o_orderkey, n_nationkey}
          List(new Pair(lines(0), nBroadcast.value.get(nationkey.get)))
        } else {
          List()
        }
      })

      val lineitems = lineitem
      .flatMap(line => {
        val lines = line.split("\\|")
        List(new Pair(lines(0), lines(10)))
      })

      val grouped = o
      .cogroup(lineitems)
      .flatMap(data => {
        val o_data = data._2._1
        val l_data = data._2._2
        val o_iterable = o_data.map(x => x.get)
        val l_iterable = l_data.map(x => (o_iterable.mkString(" "), x.substring(0, 7)))
        if(!o_iterable.isEmpty && o_iterable != null) {
          l_iterable.toList
        } else {
          List()
        }
      })
      .keyBy(z => (z._1, z._2))
      .groupByKey()
      .sortBy(k => k._1._2)
      .collect()

      for(data <- grouped) {
        println("(" + data._1._2 + "," + data._1._1 + "," + data._2.count(x => true) + ")")
      }
    } else {
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
      val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd

      val n = nationRDD
      .map(lines => {
        // emit n_nationkey, n_name
        new Pair(lines(0), lines(1))
      })

      val nBroadcast = sc.broadcast(n.collectAsMap())

      val c = customerRDD
      .flatMap(lines => {
        // emit c_custkey, c_nationkey if country is Canada or United States
        if (nBroadcast.value.get(lines(3)).get != null &&
            (nBroadcast.value.get(lines(3)).get == "CANADA" ||
             nBroadcast.value.get(lines(3)).get == "UNITED STATES")) {
          List(new Pair(lines(0), lines(3)))
        } else {
          List()
        }
      })

      val cBroadcast = sc.broadcast(c.collectAsMap())

      val o = ordersRDD
      .flatMap(lines => {
        val nationkey = cBroadcast.value.get(lines(1))
        if (nationkey != null && !nationkey.isEmpty) {
          // emit {o_orderkey, n_nationkey}
          List(new Pair(lines(0), nBroadcast.value.get(nationkey.get)))
        } else {
          List()
        }
      })

      val lineitems = lineitemRDD
      .flatMap(lines => {
        List(new Pair(lines(0), lines(10)))
      })

      val grouped = o
      .cogroup(lineitems)
      .flatMap(data => {
        val o_data = data._2._1
        val l_data = data._2._2
        val o_iterable = o_data.map(x => x.get)
        val l_iterable = l_data.map(x => (o_iterable.mkString(" "), x.toString.substring(0, 7)))
        if(!o_iterable.isEmpty && o_iterable != null) {
          l_iterable.toList
        } else {
          List()
        }
      })
      .keyBy(z => (z._1, z._2))
      .groupByKey()
      .sortBy(k => k._1._2)
      .collect()

      for(data <- grouped) {
        println("(" + data._1._2 + "," + data._1._1 + "," + data._2.count(x => true) + ")")
      }
    }
  }
}
