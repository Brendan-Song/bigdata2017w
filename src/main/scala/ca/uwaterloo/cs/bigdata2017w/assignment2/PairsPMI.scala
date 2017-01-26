package ca.uwaterloo.cs.bigdata2017w.assignment2

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

object PairsPMI extends Tokenizer {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Output: " + args.output())
		log.info("Number of reducers: " + args.reducers())

		val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Pairs")

		val sc = new SparkContext(conf)

		val outputDir = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

		val textFile = sc.textFile(args.input())
		val accum = sc.longAccumulator("Line count")

		val threshold = args.threshold()

		val counts = textFile
		// count words
		.flatMap(line => {
			val tokens = tokenize(line)
			val words: Set[String] = Set()
			val output: ListBuffer[Pair[Pair[String, String], Int]] = ListBuffer()
			for (i <- 0 until tokens.length if i < 40) yield {
				val word = tokens(i)
				val pmikey = new Pair[Pair[String, String], Int](new Pair[String, String](word, "*"), 1)
				if (!words(word)) {
					words += word
					output += pmikey
				}
			}
			output += new Pair[Pair[String, String], Int](new Pair[String, String]("*", "*"), 1)
			accum.add(1)
			output.toList
		})
		.reduceByKey(_ + _)

		val countsBroadcast = sc.broadcast(counts.collectAsMap())

		val pmi = textFile
		// count pairs
		.flatMap(line => {
			val tokens = tokenize(line)
			val foundLeft: Set[String] = Set()
			val output: ListBuffer[Pair[Pair[String, String], Int]] = ListBuffer()
			for (i <- 0 until tokens.length if i < 40) {
				if (!foundLeft(tokens(i))) {
					foundLeft += tokens(i)
					val foundRight: Set[String] = Set()
					for (j <- 0 until tokens.length if j < 40) {
						if (i != j && !(tokens(i) == tokens(j))) {
							if (!foundRight(tokens(j))) {
								foundRight += tokens(j)
								output += new Pair[Pair[String, String], Int](new Pair[String, String](tokens(i), tokens(j)), 1)
							}
						}
					}
				}
			}
			output.toList
		})
		.reduceByKey(_ + _)
		// account for threshold
		.filter{ case ((w1, w2), value) => value.toInt >= threshold }
		// calculate PMIs
		.map{ case ((w1, w2), value) => {
			val count: Int = countsBroadcast.value.get(new Pair[String, String]("*", "*")).get
			val d1: Int = countsBroadcast.value.get(new Pair[String, String](w1.toString, "*")).get
			val d2: Int = countsBroadcast.value.get(new Pair[String, String](w2.toString, "*")).get
			val numerator = count * value.toInt
			val pmiF = log10(numerator.toFloat / (d1.toFloat * d2.toFloat)).toFloat
			val pmiValue = new Pair[Float, Int](pmiF, value)
			"(" + w1 + "," + w2 + ") (" + pmiF + "," + value + ")"
		}}
		.saveAsTextFile(args.output())
	}
}
