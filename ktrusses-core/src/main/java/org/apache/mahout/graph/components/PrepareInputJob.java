package org.apache.mahout.graph.components;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.graph.common.SimplifyGraphJob;
import org.apache.mahout.graph.model.UndirectedEdge;
import org.apache.mahout.graph.model.Vertex;

/**
 * Prepares the input for the {@link FindComponentsJob }. This includes:
 * <ul>
 * <li>simplifying the graph with {@link SimplifyGraphJob }</li>
 * <li>converting the format</li>
 * </ul>
 */
public class PrepareInputJob extends AbstractJob {

  /**
   * Converts the output format from {@link SimplifyGraphJob } or the
   * {@link FindKTrussesJob.DropUnsupportedEdgesReducer } to the input format for
   * {@link FindComponentsJob }.
   * 
   */
  public static class PrepareInputMapper extends
      Mapper<UndirectedEdge, NullWritable, Vertex, FlaggedVertex> {
    @Override
    public void map(UndirectedEdge edge, NullWritable nw, Context ctx)
        throws IOException, InterruptedException {
      Vertex from = edge.getFirstVertex();
      Vertex to = edge.getSecondVertex();
      ctx.write(from, FlaggedVertex.createUndirectedEdge(to));
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PrepareInputJob(), args);
  }

  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();

    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }

    Path inputPath = getInputPath();
    Path outputPath = getOutputPath();
    Path tempDirPath = new Path(parsedArgs.get("--tempDir"));

    AtomicInteger currentPhase = new AtomicInteger();
    Configuration conf = new Configuration();

    Path simplifyInputPath = inputPath;
    Path simplifyOutputPath = new Path(tempDirPath + "/simplify"
        + System.currentTimeMillis());

    if (shouldRunNextPhase(parsedArgs, currentPhase)) {
      /*
       * Simplify the graph first
       */
      SimplifyGraphJob simplifyGraphJob = new SimplifyGraphJob();
      simplifyGraphJob.setConf(conf);
      simplifyGraphJob.run(new String[] { "--input",
          simplifyInputPath.toString(), "--output",
          simplifyOutputPath.toString(), "--tempDir", tempDirPath.toString() });
    }
    if (shouldRunNextPhase(parsedArgs, currentPhase)) {
      /*
       * Convert format
       */
      Job convertFromat = prepareJob(simplifyOutputPath, outputPath,
          SequenceFileInputFormat.class, PrepareInputMapper.class,
          Vertex.class, FlaggedVertex.class, Reducer.class, Vertex.class,
          FlaggedVertex.class, SequenceFileOutputFormat.class);
      convertFromat.waitForCompletion(true);
    }
    return 1;
  }
}
