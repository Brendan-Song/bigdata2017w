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

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    if (args.text()) {
      // lineitem.tbl
      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val shipdate = args.date()

      val l = lineitem
      .map(line => line.split("\\|"))
      .filter(lines => lines(10).contains(shipdate))
      .keyBy(lines => (lines(8), lines(9))) // group by l_returnflag, l_linestatus
      .groupByKey()

      val sum_qty = l
      .map(data => {
        data._2.iterator
        .map(lines => lines(4).toDouble)
        .toList
        .sum
      }).sum()

      val sum_base_price = l
      .map(data => {
        data._2.iterator
        .map(lines => lines(5).toDouble)
        .toList
        .sum
      }).sum()

      val sum_disc_price = l
      .map(data => {
        data._2.iterator
        .map(lines => (lines(5).toDouble) * (1 - lines(6).toDouble))
        .toList
        .sum
      }).sum()

      val sum_charge = l
      .map(data => {
        data._2.iterator
        .map(lines => (lines(5).toDouble) * (1 - lines(6).toDouble) * (1 + lines(7).toDouble))
        .toList
        .sum
      }).sum()

      val sum_disc = l
      .map(data => {
        data._2.iterator
        .map(lines => lines(6).toDouble)
        .toList
        .sum
      }).sum()

      for(data <- l.collect()) {
        println("(" + data._1._1 + "," + 
                data._1._2 + "," +
                sum_qty + "," +
                sum_base_price + "," + 
                sum_disc_price + "," + 
                sum_charge + "," + 
                sum_qty/data._2.count(x => true) + "," + 
                sum_base_price/data._2.count(x => true) + "," +
                sum_disc/data._2.count(x => true) + "," + 
                data._2.count(x => true) + ")")
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
