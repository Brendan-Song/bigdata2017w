package ca.uwaterloo.cs.bigdata2017w.assignment1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PairsPMI extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(PairsPMI.class);

	// Count lines and words
	public static final class PreMapper extends Mapper<LongWritable, Text, PairOfStrings, LongWritable> {
		private static final LongWritable ONE = new LongWritable(1);
		private static final PairOfStrings PMIKEY = new PairOfStrings();

		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		HashMap<PairOfStrings, Boolean> hash = new HashMap<PairOfStrings, Boolean>();
		List<String> tokens = Tokenizer.tokenize(value.toString());

		for (int i = 0; i < tokens.size() && i < 40; i++) {
			PMIKEY.set(tokens.get(i), "*");
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

	public static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, LongWritable> {
		private static final LongWritable ONE = new LongWritable(1);
		private static final PairOfStrings PMIKEY = new PairOfStrings();

		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		List<String> tokens = Tokenizer.tokenize(value.toString());

		HashMap<String, Boolean> foundLeft = new HashMap<String, Boolean>();
		for (int i = 0; i < tokens.size() && i < 40; i++) {
			if (!foundLeft.containsKey(tokens.get(i))) {
				foundLeft.put(tokens.get(i), true);
				HashMap<String, Boolean> foundRight = new HashMap<String, Boolean>();
				for (int j = 0; j < tokens.size() && j < 40; j++) {
					// co-occurring pair
					if (i != j && !tokens.get(i).equals(tokens.get(j))) {
						if (!foundRight.containsKey(tokens.get(j))) {
							foundRight.put(tokens.get(j), true);
							PMIKEY.set(tokens.get(i), tokens.get(j));
							context.write(PMIKEY, ONE);
						}
					}
				}
			}
		}
		}
	}

	public static final class MyCombiner extends Reducer<PairOfStrings, LongWritable, PairOfStrings, LongWritable> {
		private static final LongWritable COUNT = new LongWritable();

		@Override
		public void reduce(PairOfStrings key, Iterable<LongWritable> values, Context context)
		throws IOException, InterruptedException {
		int sum = 0;
		Iterator<LongWritable> iter = values.iterator();
		while(iter.hasNext()) {
			sum += iter.next().get();
		}
		COUNT.set(sum);
		context.write(key, COUNT);
		}
	}

	public static final class PreReducer extends Reducer<PairOfStrings, LongWritable, PairOfStrings, LongWritable> {
		private static final LongWritable COUNT = new LongWritable();

		@Override
		public void reduce(PairOfStrings key, Iterable<LongWritable> values, Context context)
		throws IOException, InterruptedException {
		int sum = 0;
		Iterator<LongWritable> iter = values.iterator();
		while(iter.hasNext()) {
			sum += iter.next().get();
		}
		COUNT.set(sum);
		context.write(key, COUNT);
		}
	}


	public static final class MyReducer extends Reducer<PairOfStrings, LongWritable, PairOfStrings, PairOfFloatInt> {
		private static final PairOfFloatInt PMIVALUE = new PairOfFloatInt();
		private static HashMap<String, Integer> hash = new HashMap<String, Integer>();
		private int lines = 0;
		private int threshold = 10;

		@Override
		public void setup(Context context) throws IOException {
			threshold = context.getConfiguration().getInt("threshold", 10);
			Path path = new Path("pairsbin");
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FileStatus[] fileList = fs.listStatus(path);
			for (FileStatus file : fileList) {
				if (!file.isDir() && file.getPath().toString().contains("pairsbin/part-r-")) {
					Path fp = file.getPath();
					SequenceFile.Reader reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(fp));

					PairOfStrings key = new PairOfStrings();
					LongWritable value = new LongWritable();
					while (reader.next(key, value)) {
						String left = key.getLeftElement();
						String right = key.getRightElement();

						if (left.equals("*")) {
							lines = Integer.parseInt(value.toString());
						} else {
							hash.put(left, Integer.parseInt(value.toString()));
						}
					}

					reader.close();
				}
			}
		}

		@Override
		public void reduce(PairOfStrings key, Iterable<LongWritable> values, Context context)
		throws IOException, InterruptedException {
		int count = 0;
		Iterator<LongWritable> iter = values.iterator();
		while (iter.hasNext()) {
			count += iter.next().get();
		}

		if (count >= threshold) {
			float d1 = hash.get(key.getLeftElement());
			float d2 = hash.get(key.getRightElement());
			float numerator = count * lines;
			float pmi = (float)(Math.log10(numerator / (d1 * d2)));
			PairOfFloatInt pmiValue = new PairOfFloatInt(pmi, count);
			context.write(key, pmiValue);
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
		FileOutputFormat.setOutputPath(job, new Path("pairsbin"));

		job.setMapOutputKeyClass(PairOfStrings.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(PairOfStrings.class);
		job.setOutputValueClass(LongWritable.class);
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
		Path outputDir = new Path("pairsbin");
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
		postJob.setMapOutputValueClass(LongWritable.class);
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
