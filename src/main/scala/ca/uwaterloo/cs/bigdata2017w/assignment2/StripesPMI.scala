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

object StripesPMI extends Tokenizer {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Output: " + args.output())
		log.info("Number of reducers: " + args.reducers())

		val conf = new SparkConf().setAppName("Stripes PMI")

		val sc = new SparkContext(conf)

		val outputDir = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

		val textFile = sc.textFile(args.input())

		val threshold = args.threshold()

		val counts = textFile
		// count words
		.flatMap(line => {
			val tokens = tokenize(line)
			val words: Set[String] = Set()
			val output: ListBuffer[Pair[String, Int]] = ListBuffer()
			for (i <- 0 until tokens.length if i < 40) yield {
				val word = tokens(i)
				if (!words(word)) {
					words += word
					output += new Pair[String, Int](word, 1)
				}
			}
			output += new Pair[String, Int]("*", 1)
			output.toList
		})
		.reduceByKey(_ + _)

		val countsBroadcast = sc.broadcast(counts.collectAsMap())

		val pmi = textFile
		// count co-occurrences
		.flatMap(line => {
			val tokens = tokenize(line)
			val stripes: Map[String, Map[String, Int]] = Map()
			val output: ListBuffer[Pair[Pair[String, String], Int]] = ListBuffer()
			
			if (tokens.length > 1) {
				for (i <- 0 until tokens.length if i < 40) {
					val found: Set[String] = Set()
					val w1 = tokens(i)
					for (j <- 0 until tokens.length if i < 40) {
						val w2 = tokens(j)
						if (i != j && !(w1 == w2)) {
							if (!found.contains(w2)) {
								if (stripes.contains(w1)) {
									val stripe: Map[String, Int] = stripes.get(w1).get
									if (stripe.contains(w2)) {
										val c = stripe.get(w2)
										stripe.put(w2, c.get + 1)
									} else {
										stripe.put(w2, 1)
									}
									stripes.put(w1, stripe)
								} else {
									val stripe: Map[String, Int] = Map()
									stripe.put(w2, 1)
									stripes.put(w1, stripe)
								}
							}
						}
						found += w2
					}
					found += w1
				}
			}
			val data = for ((k, v) <- stripes) yield {
				(k, v)
			}
			data
		})
		// reduce maps - essentially HMapStFW.plus()
		.reduceByKey((m1, m2) => {
			val m: Map[String, Int] = Map()
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
		// calculate PMIs
		.map{case (key, values) => {
			val m: Map[String, Pair[Float, Int]] = Map()
			for ((k, v) <- values) { 
				val lines: Int = countsBroadcast.value.get("*").get
				val d1: Int = countsBroadcast.value.get(key).get
				val d2: Int = countsBroadcast.value.get(k).get
				val numerator = v * lines
				val pmiF = log10(numerator.toFloat / (d1.toFloat * d2.toFloat)).toFloat
				val pmiValue = new Pair[Float, Int](pmiF, v)
				m.put(k, pmiValue)
			}
			(key, m)
		}}
		// get it to the right format
		.map{case (key, m) => {
			var str: String = ""
			for ((k, v) <- m) {
				str = str + k + "=(" + v._1 + "," + v._2 + "), "
			}
			"" + key + " {" + str.dropRight(2) + "}" 
		}}
		.saveAsTextFile(args.output())
	}
}
