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

	public static final class PreMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private static final LongWritable ONE = new LongWritable(1);
		private static final Text PMIKEY = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		HashMap<Text, Boolean> hash = new HashMap<Text, Boolean>();
		List<String> tokens = Tokenizer.tokenize(value.toString());

		for (int i = 0; i < tokens.size() && i < 40; i++) {
			PMIKEY.set(tokens.get(i));
			if (!hash.containsKey(PMIKEY)) {
				context.write(PMIKEY, ONE);
				hash.put(PMIKEY, true);
			}
		}

		// count lines
		PMIKEY.set("*");
		context.write(PMIKEY, ONE);
		}
	}

	public static final class MyMapper extends Mapper<LongWritable, Text, Text, HMapStFW> {
		private static final Text PMIKEY = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		HashMap<String, HMapStFW> stripes = new HashMap<String, HMapStFW>();
		List<String> tokens = Tokenizer.tokenize(value.toString());

		for (int i = 0; i < tokens.size() && i < 40; i++) {
			HashMap<String, Boolean> found = new HashMap<String, Boolean>();
			String w1 = tokens.get(i);
			for (int j = 0; j < tokens.size() && j < 40; j++) {
				String w2 = tokens.get(j);
				// co-occurring pair
				if (i != j && !w1.equals(w2)) {
					if (!found.containsKey(w2)) {
						if (stripes.containsKey(w1)) {
							HMapStFW stripe = stripes.get(w1);
							if (stripe.containsKey(w2)) {
								stripe.put(w2, stripe.get(w2) + 1.0f);
							} else {
								stripe.put(w2, 1.0f);
							}
							stripes.put(w1, stripe);
						} else {
							HMapStFW stripe = new HMapStFW();
							stripe.put(w2, 1.0f);
							stripes.put(w1, stripe);
						}
					}
				}
			}
		}

		for (String t : stripes.keySet()) {
			PMIKEY.set(t);
			context.write(PMIKEY, stripes.get(t));
		}
		}
	}

	public static final class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
		private static final LongWritable COUNT = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
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

	public static final class PreReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private static final LongWritable COUNT = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
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


	public static final class MyReducer extends Reducer<Text, HMapStFW, Text, HashMap<String, PairOfFloatInt>> {
		private static final PairOfFloatInt PMIVALUE = new PairOfFloatInt();
		private static HashMap<String, PairOfFloatInt> stripes = new HashMap<String, PairOfFloatInt>();
		private static HashMap<String, Integer> counts = new HashMap<String, Integer>();
		private int lines = 0;
		private int threshold = 10;

		@Override
		public void setup(Context context) throws IOException {
			threshold = context.getConfiguration().getInt("threshold", 10);
			String tmpPath = "stripesbin/part-r-00000";
			Path fp = new Path(tmpPath);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			SequenceFile.Reader reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(fp));

			Text key = new Text();
			LongWritable value = new LongWritable();
			while (reader.next(key, value)) {
				if (key.toString().equals("*")) {
					lines = Integer.parseInt(value.toString());
				} else {
					counts.put(key.toString(), Integer.parseInt(value.toString()));
				}
			}

			reader.close();
		}

		@Override
		public void reduce(Text key, Iterable<HMapStFW> values, Context context)
		throws IOException, InterruptedException {
		HMapStFW stripe = new HMapStFW();
		Iterator<HMapStFW> iter = values.iterator();
		while (iter.hasNext()) {
			stripe.plus(iter.next());
		}

		for (String word : stripe.keySet()) {
			int count = stripe.get(word);
			if (count >= threshold) {
				float d1 = counts.get(key.toString());
				float d2 = counts.get(word);
				float numerator = stripe.get(word) * lines;
				float pmi = (float)(Math.log10(numerator / (d1 * d2)));
				PMIVALUE.set(pmi, stripe.get(word));
				stripes.put(word, PMIVALUE);
			}
		}
		context.write(key, stripes);
		}

		private StripesPMI() {}

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

			LOG.info("Tool: " + StripesPMI.class.getSimpleName());
			LOG.info(" - input path: " + args.input);
			LOG.info(" - output path: " + args.output);
			LOG.info(" - number of reducers: " + args.numReducers);
			LOG.info(" - threshold: " + args.threshold);

			Configuration conf = getConf();
			Job job = Job.getInstance(conf);
			job.setJobName(StripesPMI.class.getSimpleName());
			job.setJarByClass(StripesPMI.class);

			job.setNumReduceTasks(args.numReducers);

			FileInputFormat.setInputPaths(job, new Path(args.input));
			FileOutputFormat.setOutputPath(job, new Path("bin"));

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setOutputKeyClass(Text.class);
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
			Path outputDir = new Path("stripesbin");
			FileSystem.get(conf).delete(outputDir, true);

			long startTime = System.currentTimeMillis();
			job.waitForCompletion(true);
			LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

			Job postJob = Job.getInstance(conf);
			postJob.setJobName(StripesPMI.class.getSimpleName());
			postJob.setJarByClass(StripesPMI.class);

			postJob.getConfiguration().setInt("threshold", args.threshold);
			postJob.setNumReduceTasks(args.numReducers);

			FileInputFormat.setInputPaths(postJob, new Path(args.input));
			FileOutputFormat.setOutputPath(postJob, new Path(args.output));

			postJob.setMapOutputKeyClass(Text.class);
			postJob.setMapOutputValueClass(HMapStFW.class);
			postJob.setOutputKeyClass(Text.class);
			postJob.setOutputValueClass(HashMap<String, HMapStFW>.class);
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
			ToolRunner.run(new StripesPMI(), args);
		}
	}
