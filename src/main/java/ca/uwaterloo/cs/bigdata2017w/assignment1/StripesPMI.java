package ca.uwaterloo.cs.bigdata2017w.assignment1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
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
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.map.HMapStFW;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StripesPMI extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(StripesPMI.class);

	public static final class PreMapper extends Mapper<LongWritable, Text, Text, HMapStFW> {
		private static final IntWritable ONE = new IntWritable(1);
		private static final Text TEXT = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		Map<String, HMapStFW> stripes = new HashMap<>();
		List<String> tokens = Tokenizer.tokenize(value.toString());

		for (int i = 0; i < tokens.size() && i < 40; i++) {
			PMIKEY.set("*", tokens.get(i));
			if (!hash.containsKey(PMIKEY)) {
				context.write(PMIKEY, ONE);
				hash.put(PMIKEY, true);
			}
		}

		// count lines
		PMIKEY.set("*", "*");
		context.write(PMIKEY, ONE);
		}
	}

	public static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
		private static final IntWritable ONE = new IntWritable(1);
		private static final PairOfStrings PMIKEY = new PairOfStrings();

		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		HashMap<PairOfStrings, Boolean> hash = new HashMap<PairOfStrings, Boolean>();
		List<String> tokens = Tokenizer.tokenize(value.toString());

		for (int i = 0; i < tokens.size() && i < 40; i++) {
			for (int j = 0; j < tokens.size() && j < 40; j++) {
				// co-occurring pair
				if (i != j && !tokens.get(i).equals(tokens.get(j))) {
					PMIKEY.set(tokens.get(i), tokens.get(j));
					if (!hash.containsKey(PMIKEY) || !hash.get(PMIKEY)) {
						context.write(PMIKEY, ONE);
						hash.put(PMIKEY, true);
					}
				}
			}
		}
		}
	}

	public static final class MyCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
		private static final IntWritable COUNT = new IntWritable();

		@Override
		public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> iter = values.iterator();
		while(iter.hasNext()) {
			sum += iter.next().get();
		}
		COUNT.set(sum);
		context.write(key, COUNT);
		}
	}

	public static final class PreReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
		private static final IntWritable COUNT = new IntWritable();

		@Override
		public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> iter = values.iterator();
		while(iter.hasNext()) {
			sum += iter.next().get();
		}
		COUNT.set(sum);
		context.write(key, COUNT);
		}
	}


	public static final class MyReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloatInt> {
		private static final PairOfFloatInt PMIVALUE = new PairOfFloatInt();
		private static HashMap<String, Integer> hash = new HashMap<String, Integer>();
		private int lines = 0;
		private int threshold = 10;

		@Override
		public void setup(Context context) throws IOException {
			threshold = context.getConfiguration().getInt("threshold", 10);
			String tmpPath = "bin/part-r-00000";
			Path fp = new Path(tmpPath);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			SequenceFile.Reader reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(fp));

			PairOfStrings key = new PairOfStrings();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				String left = key.getLeftElement();
				String right = key.getRightElement();

				if (right.equals("*")) {
					lines = Integer.parseInt(value.toString());
				} else {
					hash.put(right, Integer.parseInt(value.toString()));
				}
			}

			reader.close();
		}

		@Override
		public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
		int count = 0;
		Iterator<IntWritable> iter = values.iterator();
		while (iter.hasNext()) {
			count += iter.next().get();
		}

		if (count >= threshold) {
			float d1 = hash.get(key.getLeftElement());
			float d2 = hash.get(key.getRightElement());
			float numerator = count * lines;
			float pmi = (float)(Math.log10(numerator / (d1 * d2)));
			PMIVALUE.set(pmi, count);
			context.write(key, PMIVALUE);
		}
		}
	}

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
		FileOutputFormat.setOutputPath(job, new Path("bin"));

		job.setMapOutputKeyClass(PairOfStrings.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(PairOfStrings.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapperClass(PreMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(PreReducer.class);

		job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
		job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

		// Delete the output directory if it exists already.
		Path outputDir = new Path("bin");
		FileSystem.get(conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		Job postJob = Job.getInstance(conf);
		postJob.setJobName(PairsPMI.class.getSimpleName());
		postJob.setJarByClass(PairsPMI.class);

		postJob.getConfiguration().setInt("threshold", args.threshold);
		postJob.setNumReduceTasks(args.numReducers);

		FileInputFormat.setInputPaths(postJob, new Path(args.input));
		FileOutputFormat.setOutputPath(postJob, new Path(args.output));

		postJob.setMapOutputKeyClass(PairOfStrings.class);
		postJob.setMapOutputValueClass(IntWritable.class);
		postJob.setOutputKeyClass(PairOfStrings.class);
		postJob.setOutputValueClass(PairOfFloatInt.class);
		postJob.setOutputFormatClass(TextOutputFormat.class);

		postJob.setMapperClass(MyMapper.class);
		postJob.setCombinerClass(MyCombiner.class);
		postJob.setReducerClass(MyReducer.class);

		postJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
		postJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
		postJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
		postJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
		postJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

		Path postOutputDir = new Path(args.output);
		FileSystem.get(conf).delete(postOutputDir, true);

		postJob.waitForCompletion(true);
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
