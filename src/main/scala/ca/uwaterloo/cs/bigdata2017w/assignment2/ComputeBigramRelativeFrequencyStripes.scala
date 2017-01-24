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
import tl.lin.data.map.HMapStFW
import tl.lin.data.map.HMapStIW
import tl.lin.data.map.MapKF
import tl.lin.data.pair.PairOfStrings

object ComputeBigramRelativeFrequencyStripes extends Configured with Tool with WritableConversions with Tokenizer {
	val log = Logger.getLogger(getClass.getName)

	class MyMapper extends Mapper[LongWritable, Text, Text, HMapStFW] {
		val TEXT = new Text()
		override def map(key: LongWritable, value: Text,
			context: Mapper[LongWritable, Text, Text, HMapStFW]#Context) = {
				val stripes = new HashMap[String, HMapStFW]()
				val words = tokenize(value)
				for(i <- 1 until words.length) {
					var prev = words(i-1)
					var curr = words(i)
					if (stripes.contains(prev)) {
						stripe = stripes.get(prev)
						if (stripe.contains(curr)) {
							stripe.put(curr, stripe.get(curr) + 1.0f)
						} else {
							stripe.put(curr, 1.0f)
						}
					} else {
						stripe = new HMapStFW()
						stripe.put(curr, 1.0f)
						stripes.put(prev, stripe)
					}
				}
				
				for ((k, v) <- stripes) {
					TEXT.set(k)
					context.write(TEXT, v)
				}
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

       	class MyCombiner extends Reducer[Text, HMapStFW, Text, HMapStFW] {
		val SUM = new FloatWritable()

		override def reduce(key: Text, values: java.lang.Iterable[HMapStFW],
			context: Reducer[Text, HMapStFW, Text, HMapStFW]#Context) = {
				val map = new HMapStFW()
				for (value <- values.asScala) {
					map.plus(value)
				}
				context.write(key, map)
		}
	}

	class MyReducer extends Reducer[Text, HMapStFW, Text, HMapStFW] {
		val VALUE = new FloatWritable()
		var marginal = 0.0f
		override def reduce(key: Text, values: java.lang.Iterable[HMapStFW],
			context: Reducer[Text, HMapStFW, Text, HMapStFW]#Context) = {
				val map = new HMapStFW()
				for (value <- values.asScala) {
					map.plus(value)
				}

				var sum = 0.0f
				for ((k, v) <- map) {
					sum += v.getValue()
				}
				for ((k, v) <- map) {
					map.put(k, v / sum)
				}
				
				context.write(key, map)
		}
	}

	override def run(argv: Array[String]) : Int = {
		val args = new Conf(argv)

		log.info("Input: " + args.input())
		log.info("Output: " + args.output())
		log.info("Number of reducers: " + args.reducers())
		log.info("Number of executors: " + args.executors())
		log.info("Number of cores: " + args.cores())
		log.info("Amount of memory: " + args.memory())


		val conf = getConf()
		val job = Job.getInstance(conf)

		FileInputFormat.addInputPath(job, new Path(args.input()))
		FileOutputFormat.setOutputPath(job, new Path(args.output()))

		job.setJobName("Compute Bigram Relative Frequency Stripes")
		job.setJarByClass(this.getClass)

		job.setMapOutputKeyClass(classOf[Text])
		job.setMapOutputValueClass(classOf[HMapStFW])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[HMapStFW])
		job.setOutputFormatClass(classOf[TextOutputFormat[Text, HMapStFW]])

		job.setMapperClass(classOf[MyMapper])
		job.setCombinerClass(classOf[MyCombiner])
		job.setReducerClass(classOf[MyReducer])

		job.setNumReduceTasks(args.reducers())

		val outputDir = new Path(args.output())
		FileSystem.get(conf).delete(outputDir, true)

		val startTime = System.currentTimeMillis()
		job.waitForCompletion(true)
		
		log.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds")
		println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds")

		return 0
	}

	def main(args: Array[String]) {
		ToolRunner.run(this, args)
	}
}
