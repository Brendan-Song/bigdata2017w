package ca.uwaterloo.cs.bigdata2017w.assignment3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfWritables;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
    private static final PairOfStringInt KEY = new PairOfStringInt();
    private static final IntWritable VALUE = new IntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        KEY.set(e.getLeftElement(), (int)docno.get());
        VALUE.set(e.getRightElement());
        // emit (tuple <t, n>, tf H{t})
        context.write(KEY, VALUE);
      }
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {
    private static final IntWritable DF = new IntWritable();
    private static final Text TERM = new Text();
    private static final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    private static final DataOutputStream dataStream = new DataOutputStream(byteStream);
    
    String lastWord = "";
    int lastSeen = 0;
    int df = 0;

    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();

      // if t != t_prev && t_prev != 0
      if (!key.getLeftElement().equals(lastWord) && !lastWord.equals("")) {
        // emit (term t, postings P)
        dataStream.flush();
        byteStream.flush();

        ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream(byteStream.size());
        DataOutputStream dataBuffer = new DataOutputStream(byteBuffer);

        WritableUtils.writeVInt(dataBuffer, df);
        dataBuffer.write(byteStream.toByteArray());

        TERM.set(lastWord);
        context.write(TERM, new BytesWritable(byteBuffer.toByteArray()));

        // P.reset
        byteStream.reset();
        lastSeen = 0;
        df = 0;
      }

      // P.append(<n, f>)
      while (iter.hasNext()) {
        df++;
        // encode d-gap instead of docno
        WritableUtils.writeVInt(dataStream, (key.getRightElement() - lastSeen));
        WritableUtils.writeVInt(dataStream, iter.next().get());
        lastSeen = key.getRightElement();
      }

      // t_prev <- t
      lastWord = key.getLeftElement();
    }

    @Override
    public void cleanup(Context context)
        throws IOException, InterruptedException {
      // emit (term t, postings P)
      dataStream.flush();
      byteStream.flush();

      ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream(byteStream.size());
      DataOutputStream dataBuffer = new DataOutputStream(byteBuffer);

      WritableUtils.writeVInt(dataBuffer, df);
      dataStream.write(byteStream.toByteArray());

      TERM.set(lastWord);
      context.write(TERM, new BytesWritable(byteBuffer.toByteArray()));

      byteStream.close();
      dataStream.close();
    }
  }

  private static class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
	@Override
	public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
    // return Hash(t) mod NumOfReducers
		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
  }

  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[int]", required = false, usage = "num reducers")
    int reducers = 1;	
  }

  /**
   * Runs this tool.
   */
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

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.reducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.reducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
