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

object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)

    // lineitem.tbl
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
    // orders.tbl
    val orders = sc.textFile(args.input() + "/orders.tbl")
    // customer.tbl
    val customer = sc.textFile(args.input() + "/customer.tbl")
    // nation.tbl
    val nation = sc.textFile(args.input() + "/nation.tbl")
    val shipdate = args.date()

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
      // emit c_custkey, c_nationkey
      List(new Pair(lines(0), lines(3)))
    })

    val cBroadcast = sc.broadcast(c.collectAsMap())

    val o = orders
    .flatMap(line => {
      val lines = line.split("\\|")
      val nationkey = cBroadcast.value.get(lines(1))
      if (nationkey != null ) {
        // emit {o_orderkey, {n_nationkey, n_name}}
        List(new Pair(lines(0),
             new Pair(nationkey, nBroadcast.value.get(nationkey.get))))
      } else {
        List()
      }
    })

    val lineitems = lineitem
    .flatMap(line => {
      val lines = line.split("\\|")
      if(lines(10).contains(shipdate)) { 
        List(new Pair(lines(0), lines(10)))
      } else {
        List()
      }
    })

    val grouped = o
    .cogroup(lineitems)
    .flatMap(data => {
      val o_data = data._2._1
      val l_data = data._2._2
      val l_iterable = l_data.map(x => x)
      if(!l_iterable.isEmpty) {
        o_data
        .flatMap(y => {
          List(new Pair(Integer.parseInt(y._1.get), y._2.get))
        })
      } else {
        List()
      }
    })
    .keyBy(z => (z._1, z._2))
    .groupByKey()
    .sortBy(k => k._1)
    .collect()

    for(data <- grouped) {
      println("(" + data._1._1 + "," + data._1._2 + "," + data._2.count(x => true) + ")")
    }
  }
}
