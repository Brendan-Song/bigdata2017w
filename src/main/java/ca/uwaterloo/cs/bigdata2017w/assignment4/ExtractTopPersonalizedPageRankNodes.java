package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

import tl.lin.data.queue.TopScoredObjects;
import tl.lin.data.pair.PairOfObjectFloat;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
  }

  public ExtractTopPersonalizedPageRankNodes() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCE_NODES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("top").hasArg()
        .withDescription("top results").create(TOP));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
        .withDescription("source nodes").create(SOURCE_NODES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inPath = cmdline.getOptionValue(INPUT);
    String outPath = cmdline.getOptionValue(OUTPUT);
    int top = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sourceNodes = cmdline.getOptionValue(SOURCE_NODES);

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input dir: " + inPath);
    LOG.info(" - output dir: " + outPath);
    LOG.info(" - top: " + top);
    LOG.info(" - source nodes: " + sourceNodes);
    
    printTopX(inPath, sourceNodes, top);
    
    return 0;
  }

  private void printTopX(String inPath, String sourceNodes, int top)
    throws IOException, InstantiationException, IllegalAccessException {

    TopScoredObjects<Integer> results = new TopScoredObjects<Integer>(top);
    Configuration conf = new Configuration();
    Path path = new Path(inPath);
    FileSystem fs = FileSystem.get(conf);
    String[] sources = sourceNodes.split(",");

    for(FileStatus file : fs.listStatus(path)) {
      Path fPath = file.getPath();

      // avoid _SUCCESS
      if(!fPath.toString().contains("_SUCCESS")) {
        SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), fPath, conf);
        IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
        PageRankNode value = (PageRankNode) reader.getValueClass().newInstance();

        while(reader.next(key, value)) {
          results.add(key.get(), value.getPageRank());
        }
        reader.close();
      }
    }

    for(int i = 0; i < sources.length; i++) {
      System.out.println("Source: " + sources[i]);
      
      for(PairOfObjectFloat<Integer> pair : results.extractAll()) {
        int node = ((Integer) pair.getLeftElement());
        float p = (float) Math.exp(pair.getRightElement());
        System.out.println(String.format("%.5f %d", p, node));
      }
      System.out.println();
    }
  }
}
