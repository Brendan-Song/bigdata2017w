package ca.uwaterloo.cs.bigdata2017w.assignment6

import io.bespin.scala.util.Tokenizer
import io.bespin.scala.util.WritableConversions

import java.util.StringTokenizer

import scala.collection.JavaConverters._
import scala.collection.mutable._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.apache.log4j._
import org.rogach.scallop._

class EnsembleConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, method)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model directory", required = false)
  val output = opt[String](descr = "output path", required = false)
  val method = opt[String](descr = "ensemble method", required = false)
  verify()
}
