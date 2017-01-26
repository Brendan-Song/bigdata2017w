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
				val stripe: Map[String, Float] = Map()
				val tokens = tokenize(line)
				if (tokens.length > 1) {
					tokens.sliding(2).map(p => {
						stripe.put(p(1), 1.0f)
						(p(0), stripe)
					})
				} else List()
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

				//get it to the right format
				var str: String = ""
				for ((k, v) <- m) {
					str = str + k + "=" + v + ", "
				}
				"" + key + " {" + str.dropRight(2) + "}"
			}}
			.saveAsTextFile(args.output())
	}
}
