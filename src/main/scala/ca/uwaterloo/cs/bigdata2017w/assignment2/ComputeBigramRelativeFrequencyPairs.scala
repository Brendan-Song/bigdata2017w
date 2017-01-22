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
import org.rogach.scallop._
import tl.lin.data.pair.PairOfStrings

object ComputeBigramRelativeFrequencyPairs extends Configured with Tool with WritableConversions with Tokenizer {
	val log = Logger.getLogger(getClass.getName)
	val BIGRAM = new PairOfStrings()
	val ONE = new FloatWritable(1)

	class MyMapper extends Mapper[LongWritable, Text, PairOfStrings, FloatWritable] {
		override def map(key: LongWritable, value: Text,
			context: Mapper[LongWritable, Text, PairOfStrings, FloatWritable]#Context) = {
				val words = tokenize(value)
				for(i <- 1 until words.length) {
					BIGRAM.set(words(i-1), words(i))
					context.write(BIGRAM, ONE)

					BIGRAM.set(words(i-1), "*")
					context.write(BIGRAM, ONE)
				}
		}
	}

	// Same as above, except using explicit loops to look more like pseudo-code
	/*class AlternativeMapperIMC extends Mapper[LongWritable, Text, Text, IntWritable] {
		val counts = new HashMap[String, Int]().withDefaultValue(0)

		override def map(key: LongWritable, value: Text,
			context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
				for (word <- tokenize(value)) {
					counts(word) += 1
				}
		}

    override def cleanup(context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
	    for ((k, v) <- counts) {
		    context.write(k, v)
	    }
    }
	}*/

       	class MyCombiner extends Reducer[PairOfStrings, FloatWritable, PairOfStrings, FloatWritable] {
		val SUM = new FloatWritable()

		override def reduce(key: PairOfStrings, values: java.lang.Iterable[FloatWritable],
			context: Reducer[PairOfStrings, FloatWritable, PairOfStrings, FloatWritable]#Context) = {
				var sum = 0.0f
				for (value <- values.asScala) {
					sum += value
				}
				SUM.set(sum)
				context.write(key, SUM)
		}
	}

	class MyReducer extends Reducer[PairOfStrings, FloatWritable, PairOfStrings, FloatWritable] {
		val VALUE = new FloatWritable()
		var marginal = 0.0f
		override def reduce(key: PairOfStrings, values: java.lang.Iterable[FloatWritable],
			context: Reducer[PairOfStrings, FloatWritable, PairOfStrings, FloatWritable]#Context) = {
				var sum = 0.0f
				for (value <- values.asScala) {
					sum += value
				}

				if (key.getRightElement().equals("*")) {
					VALUE.set(sum)
					context.write(key, VALUE)
					marginal = sum
				} else {
					VALUE.set(sum / marginal)
					context.write(key, VALUE)
				}
		}
	}

	class MyPartitioner extends Partitioner[PairOfStrings, FloatWritable] {
		override def getPartition(key: PairOfStrings, value: FloatWritable, numReduceTasks: Int): Int = {
			return (key.getLeftElement.hashCode & Integer.MAX_VALUE) % numReduceTasks
		}
	}

	override def run(argv: Array[String]) : Int = {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Output: " + args.output())
		log.info("Number of reducers: " + args.reducers())


		val conf = getConf()
		val job = Job.getInstance(conf)

		FileInputFormat.addInputPath(job, new Path(args.input()))
		FileOutputFormat.setOutputPath(job, new Path(args.output()))

		job.setJobName("Compute Bigram Relative Frequency Pairs")
		job.setJarByClass(this.getClass)

		job.setMapOutputKeyClass(classOf[PairOfStrings])
		job.setMapOutputValueClass(classOf[FloatWritable])
		job.setOutputKeyClass(classOf[PairOfStrings])
		job.setOutputValueClass(classOf[FloatWritable])
		job.setOutputFormatClass(classOf[TextOutputFormat[PairOfStrings, FloatWritable]])

		job.setMapperClass(classOf[MyMapper])
		job.setCombinerClass(classOf[MyCombiner])
		job.setReducerClass(classOf[MyReducer])
		job.setPartitionerClass(classOf[MyPartitioner])

		job.setNumReduceTasks(args.reducers())

		val outputDir = new Path(args.output())
		FileSystem.get(conf).delete(outputDir, true)

		val startTime = System.currentTimeMillis()
		job.waitForCompletion(true)
		println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds")

		return 0
	}

	def main(args: Array[String]) {
		ToolRunner.run(this, args)
	}
}
