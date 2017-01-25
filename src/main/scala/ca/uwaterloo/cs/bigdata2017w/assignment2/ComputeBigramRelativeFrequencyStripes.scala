package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer
import io.bespin.scala.util.WritableConversions

import scala.collection.JavaConversions._
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
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import tl.lin.data.map.HMapStFW
import tl.lin.data.map.HMapStIW
import tl.lin.data.map.MapKF
import tl.lin.data.pair.PairOfStrings

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Output: " + args.output())
		log.info("Number of reducers: " + args.reducers())
		log.info("Number of executors: " + args.executors())
		log.info("Number of cores: " + args.cores())

		val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Stripes")
		val sc = new SparkContext(conf)

		val outputDir = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

		val textFile = sc.textFile(args.input())

		val counts = textFile
			// map 2 adjacent words
			.flatMap(line => {
				val stripes: Map[String, HMapStFW] = Map()
				val tokens = tokenize(line)
				if (tokens.length > 1) {
					for (i <- 1 until tokens.length) {
						val prev = tokens(i-1)
						val curr = tokens(i)
						if (stripes.contains(prev)) {
							val stripe = stripes.get(prev)
							if (stripe.contains(curr)) {
								stripe.get.put(curr, stripe.get.get(curr) + 1.0f)
							} else {
								stripe.get.put(curr, 1.0f)
							}
						} else {
							val stripe = new HMapStFW()
							stripe.put(curr, 1.0f)
							stripes.put(prev, stripe)
						}
					}
				}
				val data = for ((k, v) <- stripes) yield {
					(k, v)
				}
				data
			})
			.reduceByKey((key, value) => {
				val m = new HMapStFW()
				m.plus(value)
				m
			})
				.map{case (text, value) => {
					(text, value)
				}}
			/*.map{case (text, value) => {
				var sum = 0.0f
				val m = new HMapStFW()
				for ((k:String, v:Float) <- value) {
					sum = sum + v
				}
				for ((k:String, v:Float) <- value) {
					m.put(k, (v / sum))
				}
				(text, m)
			}}*/
			.saveAsTextFile(args.output())
			/*
			// separate adjacent words and count
			.flatMap(pair => {
				val words = pair.split(" ")
				val pairCount = new Pair[String, String](words(0), words(1))
				val wordCount = new Pair[String, String](words(0), "*")
				new Pair[Pair[String, String], Int](wordCount, 1) :: List(new Pair[Pair[String, String], Int](pairCount, 1))
			})
			// start combining pairs
			.reduceByKey(_ + _)
			// sort to get counts first
			.repartitionAndSortWithinPartitions(new PairsPartitioner(args.reducers()))
			// calculate and emit frequencies
			.map{ case ((w1, w2), value) => {
				if (w2 == "*") {
					marginal = value
					freq = value.toFloat
				} else {
					freq = (value.toFloat / marginal.toFloat)
				}
				((w1, w2), freq)
			}}
			// get it to the right format
			.map{ case ((w1, w2), freq) => {
				"(" + w1 + ", " + w2 + "), " + freq
				}
			}
			// save parts
			.saveAsTextFile(args.output())*/
	}
}
