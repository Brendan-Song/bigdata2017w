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

object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)

    if (args.text()) {
      // lineitem.tbl
      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val orders = sc.textFile(args.input() + "/orders.tbl")
      val customer = sc.textFile(args.input() + "/customer.tbl")
      val shipdate = args.date()

      val l = lineitem
      .map(line => {
        val lines = line.split("\\|")
        lines
      })
      .filter(lines => lines(10) > shipdate)
      .map(lines => {
        // emit l_orderkey, (l_extendedprice*(1-l_discount))
        (lines(0), ((lines(5).toDouble) * (1 - lines(6).toDouble)))
      })
      .keyBy(data => data._1)

      val c = customer
      .flatMap(line => {
        val lines = line.split("\\|")
        // emit c_custkey, c_name
        List(new Pair(lines(0), lines(1)))
      })

      val cBroadcast = sc.broadcast(c.collectAsMap())

      val o = orders
      .map(line => {
        val lines = line.split("\\|")
        lines
      })
      .filter(lines => lines(4) < shipdate)
      .filter(lines => !cBroadcast.value.get(lines(1)).isEmpty)
      .map(lines => {
        // emit o_orderkey, o_orderdate, o_shippriority, c_name
        (lines(0), lines(4), lines(7), cBroadcast.value.get(lines(1)).get)
      })
      .keyBy(data => data._1)

      val grouped = o
      .cogroup(l)
      .filter(data => !data._2._1.isEmpty && !data._2._2.isEmpty)
      .map(data => {
        val l_orderkey = data._1
        val revenue = data._2._2.map(x => x._2).sum
        val o_orderdate = data._2._1.map(x => x._2).mkString(" ")
        val o_shippriority = data._2._1.map(x => x._3).mkString(" ")
        val c_name = data._2._1.map(x => x._4).mkString(" ")
        (c_name, l_orderkey, o_orderdate, o_shippriority, revenue)
      })
      .keyBy(data => (data._1, data._2, data._3, data._4))
      .groupByKey()
      .map(data => ((data._1._1, data._1._2, data._1._3, data._1._4), data._2.map(x => x._5).sum))
      .sortBy(data => data._2, false)
      .take(10)

      for(data <- grouped) {
        println("(" + data._1._1 + "," +
                data._1._2 + "," +
                data._2 + "," +
                data._1._3 + "," +
                data._1._4 + ")")
      }
    } else {
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val shipdate = args.date()

      val l = lineitemRDD
      .filter(lines => lines(10).toString.contains(shipdate))
      .keyBy(lines => (lines(8), lines(9))) // group by l_returnflag, l_linestatus
      .groupByKey()

      val sum_qty = l
      .map(data => {
        data._2.iterator
        .map(lines => lines(4).toString.toDouble)
        .toList
        .sum
      }).sum()

      val sum_base_price = l
      .map(data => {
        data._2.iterator
        .map(lines => lines(5).toString.toDouble)
        .toList
        .sum
      }).sum()

      val sum_disc_price = l
      .map(data => {
        data._2.iterator
        .map(lines => (lines(5).toString.toDouble) * (1 - lines(6).toString.toDouble))
        .toList
        .sum
      }).sum()

      val sum_charge = l
      .map(data => {
        data._2.iterator
        .map(lines => (lines(5).toString.toDouble) * 
                      (1 - lines(6).toString.toDouble) *
                      (1 + lines(7).toString.toDouble))
        .toList
        .sum
      }).sum()

      val sum_disc = l
      .map(data => {
        data._2.iterator
        .map(lines => lines(6).toString.toDouble)
        .toList
        .sum
      }).sum()

      for(data <- l.collect()) {
        println("(" + data._1._1.toString + "," + 
                data._1._2.toString + "," +
                sum_qty + "," +
                sum_base_price + "," + 
                sum_disc_price + "," + 
                sum_charge + "," + 
                sum_qty/data._2.count(x => true) + "," + 
                sum_base_price/data._2.count(x => true) + "," +
                sum_disc/data._2.count(x => true) + "," + 
                data._2.count(x => true) + ")")
      }
    }
  }
}
