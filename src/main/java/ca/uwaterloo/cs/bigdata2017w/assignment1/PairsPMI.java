package ca.uwaterloo.cs.bigdata2017w.assignment1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfFloats;
import tl.lin.data.pair.PairOfStrings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PairsPMI extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(PairsPMI.class);

	public static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
		private static final FloatWritable ONE = new FloatWritable(1);
		private static final PairOfStrings PMIKEY = new PairOfStrings();

		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		HashMap<PairOfStrings, Boolean> hash = new HashMap<PairOfStrings, Boolean>();
		List<String> tokens = Tokenizer.tokenize(value.toString());

		// get total count of lines
		PMIKEY.set("*", "*");
		context.write(PMIKEY, ONE);

		if (tokens.size() < 2) return;
		for (int i = 0; i < tokens.size() && i < 40; i++) {
			for (int j = 0; j < tokens.size() && j < 40; j++) {
				// co-occurring pair
				if (i != j && !tokens.get(i).equals(tokens.get(j))) {
					PMIKEY.set(tokens.get(i), tokens.get(j));
					if (!hash.containsKey(PMIKEY) || !hash.get(PMIKEY)) {
						context.write(PMIKEY, ONE);
						hash.put(PMIKEY, true);
					}
					// same word
				} else if (i == j) {
					PMIKEY.set("*", tokens.get(i));
					if (!hash.containsKey(PMIKEY) || !hash.get(PMIKEY)) {
						context.write(PMIKEY, ONE);
						hash.put(PMIKEY, true);
					}
				}
			}
		}
		}
	}

	public static final class MyCombiner extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
		private static final FloatWritable COUNT = new FloatWritable();

		@Override
		public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
		throws IOException, InterruptedException {
		float sum = 0.0f;
		Iterator<FloatWritable> iter = values.iterator();
		while(iter.hasNext()) {
			sum += iter.next().get();
		}
		COUNT.set(sum);
		context.write(key, COUNT);
		}
	}

	public static final class MyReducer extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, PairOfFloats> {
		private static final PairOfFloats PMIVALUE = new PairOfFloats();
		private static HashMap<String, Float> hash = new HashMap<String, Float>();
		private float total = 0.0f;

		@Override
		public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
		throws IOException, InterruptedException {
		float sum = 0.0f;
		Iterator<FloatWritable> iter = values.iterator();
		while (iter.hasNext()) {
			sum += iter.next().get();
		}

		// total number of lines
		if (key.getRightElement().equals("*")) {
			PMIVALUE.set(0.0f, sum);
			context.write(key, PMIVALUE);
			total = sum;
			// counts of each word
		} else if (key.getLeftElement().equals("*")) {
			PMIVALUE.set(0.0f, sum);
			context.write(key, PMIVALUE);
			hash.put(key.getRightElement(), sum / total);
		} else {
			float d1 = hash.get(key.getLeftElement());
			float d2 = hash.get(key.getRightElement());
			float numerator = sum / total;
			float pmi = (float)(Math.log10(numerator / (d1 * d2)));
			PMIVALUE.set(pmi, sum);
			context.write(key, PMIVALUE);
		}
		}
	}

	//private static final class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
	//	@Override
	//	public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
	//		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	//	}
	//}

	private PairsPMI() {}

	private static final class Args {
		@Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
			String input;

		@Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
			String output;

		@Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
			int numReducers = 1;

		@Option(name = "-threshold", metaVar = "[num]", usage = "do not show below threshold")
			int threshold = 10;
	}

	@Override
	public int run(String[] argv) throws Exception {
		final Args args = new Args();
		CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

		try {
			parser.parseArgument(argv);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return -1;
		}

		LOG.info("Tool: " + PairsPMI.class.getSimpleName());
		LOG.info(" - input path: " + args.input);
		LOG.info(" - output path: " + args.output);
		LOG.info(" - number of reducers: " + args.numReducers);
		LOG.info(" - threshold: " + args.threshold);

		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName(PairsPMI.class.getSimpleName());
		job.setJarByClass(PairsPMI.class);

		job.setNumReduceTasks(args.numReducers);

		FileInputFormat.setInputPaths(job, new Path(args.input));
		FileOutputFormat.setOutputPath(job, new Path(args.output));

		job.setMapOutputKeyClass(PairOfStrings.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(PairOfStrings.class);
		job.setOutputValueClass(PairOfFloats.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);
		//job.setPartitionerClass(MyPartitioner.class);

		// Delete the output directory if it exists already.
		Path outputDir = new Path(args.output);
		FileSystem.get(conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PairsPMI(), args);
	}
}
