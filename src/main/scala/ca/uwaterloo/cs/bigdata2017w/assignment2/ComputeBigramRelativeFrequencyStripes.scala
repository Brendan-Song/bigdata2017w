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
import org.apache.spark.Partitioner
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

		val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Stripes")
		val sc = new SparkContext(conf)

		val outputDir = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

		val textFile = sc.textFile(args.input())

		val counts = textFile
			// map words
			.flatMap(line => {
				val stripes: Map[String, Map[String, Float]] = Map()
				val tokens = tokenize(line)
				if (tokens.length > 1) {
					for (i <- 1 until tokens.length) {
						val prev = tokens(i-1)
						val curr = tokens(i)
						if (stripes.contains(prev)) {
							val stripe = stripes.get(prev)
							if (stripe.contains(curr)) {
								val c = stripe.get.get(curr) 
								stripe.get.put(curr, c.get + 1.0f)
							} else {
								stripe.get.put(curr, 1.0f)
							}
						} else {
							val stripe: Map[String, Float] = Map()
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
			// reduce maps - essentially HMapStFW.plus()
			.reduceByKey((m1, m2) => {
				val m: Map[String, Float] = Map()
				for ((k, v) <- m1) {
					if (m.contains(k)) {
						m.put(k, m.get(k).get + v)
					} else {
						m.put(k, v)
					}
				}
				for ((k, v) <- m2) {
					if (m.contains(k)) {
						m.put(k, m.get(k).get + v)
					} else {
						m.put(k, v)
					}
				}
				m
			})
			// sort
			.sortByKey(true, args.reducers())
			// calculate frequencies
			.map{case (key, values) => {
				var sum = 0.0f
				val m: Map[String, Float] = Map()
				for ((k, v) <- values) {
					sum = sum + v
				}
				for ((k, v) <- values) {
					m.put(k, (v / sum))
				}
				(key, m)
			}}
			// get it to the right format
			.map{case (key, m) => {
				var str: String = ""
				for ((k, v) <- m) {
					str = str + k + "=" + v + ", "
				}
				"" + key + " {" + str.dropRight(2) + "}"
			}}
			.saveAsTextFile(args.output())
	}
}
