package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer
import io.bespin.scala.util.WritableConversions

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
import tl.lin.data.pair.PairOfStrings

class PairsPartitioner(val partitions: Int) extends Partitioner {
	def getPartition(key: Any): Int = {
		val k = key.asInstanceOf[Pair[String, String]]
		(k._1.hashCode & Integer.MAX_VALUE) % partitions
	}

	def numPartitions(): Int = {
		partitions
	}
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf(argv)

		var marginal = 0
		var freq = 0.0f

		log.info("Input: " + args.input())
		log.info("Output: " + args.output())
		log.info("Number of reducers: " + args.reducers())

		val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Pairs")

		val sc = new SparkContext(conf)
		
		val outputDir = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

		val textFile = sc.textFile(args.input())

		val counts = textFile
				// map 2 adjacent words
				.flatMap(line => {
					val tokens = tokenize(line)
					if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList else List()
				})
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
				.saveAsTextFile(args.output())
	}
}
