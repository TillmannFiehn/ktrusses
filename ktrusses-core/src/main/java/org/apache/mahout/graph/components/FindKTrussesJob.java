/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.graph.components;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.graph.common.AugmentGraphWithDegreesJob;
import org.apache.mahout.graph.common.SimplifyGraphJob;
import org.apache.mahout.graph.components.PrepareInputJob.PrepareInputMapper;
import org.apache.mahout.graph.model.Triangle;
import org.apache.mahout.graph.model.UndirectedEdge;
import org.apache.mahout.graph.model.Vertex;
import org.apache.mahout.graph.triangles.EnumerateTrianglesJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Find <code>k</code>-trusses in a graph. This algorithm works as follows:
 * <p>
 * First simplify the graph. See
 * {@link org.apache.mahout.graph.common.SimplifyGraphJob}.
 * <p>
 * At least once and as long as the last step drops any edges:
 * <ol>
 * <li>Augment the graph with degrees. See
 * {@link org.apache.mahout.graph.common.AugmentGraphWithDegreesJob}</li>
 * <li>Enumerate the triangles of the graph. See
 * {@link org.apache.mahout.graph.common.EnumerateTrianglesJob}</li>
 * <li>For each edge record the number of triangles containing that edge and
 * keep only edges with sufficient support using classes in {@link FindKTrusses}
 * .</li>
 * </ol>
 * 
 * <p>
 * Find the components of the remaining graph, each one is a truss
 * {@link FindComponentsJob}.
 * 
 * <p>
 * The input file format is a {@link TextInputFormat} <code>Long,Long</code>
 * representing an Edge.
 * 
 * <p>
 * This job accepts the following input arguments:
 * <dl>
 * <dt>input</dt>
 * <dd>The path of the input file or directory</dd>
 * <dt>output</dt>
 * <dd>The path of output directory</dd>
 * <dt>org.apache.mahout.graph.model.Parser</dt>
 * <dd>An optional class implementing
 * {@link org.apache.mahout.graph.model.Parser} that can be used to parse a
 * variety of graphs</dd>
 * <dt>k</dt>
 * <dd>The <code>k</code> parameter of the k-trusses to find
 * </dl>
 * 
 * The output is a {@link SequenceFile} containing a {@link Membership} as key
 * and a {@link org.apache.mahout.graph.model.ZoneAssignment} as value
 * (contained in a {@link GenericGraphElement}).
 */
public class FindKTrussesJob extends AbstractJob {

  public enum Counter {
    DROPPED_EDGES
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new FindKTrussesJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();

    addOption("k", "k", "The k parameter of the k-trusses to find.");

    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }

    Path inputPath = getInputPath();
    Path outputPath = getOutputPath();
    Path tempDirPath = new Path(parsedArgs.get("--tempDir"));

    int k = Integer.parseInt(parsedArgs.get("--k")); // extract parameter

    AtomicInteger currentPhase = new AtomicInteger();
    Configuration conf = new Configuration();

    Path simplifyInputPath = inputPath;
    Path simplifyOutputPath = new Path(tempDirPath, String.valueOf(System
        .currentTimeMillis()));

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

    Path currentTrussesDirPath = simplifyOutputPath;

    if (shouldRunNextPhase(parsedArgs, currentPhase)) {
      while (true) {
        /*
         * Augment the simplified graph with degrees
         */
        // scatter the edges to each of the vertices and count degree
        Path augmentInputPath = currentTrussesDirPath;
        Path augmentOutputPath = new Path(tempDirPath, "augment"
            + String.valueOf(System.currentTimeMillis()));

        AugmentGraphWithDegreesJob augmentGraphWithDegreesJob = new AugmentGraphWithDegreesJob();
        augmentGraphWithDegreesJob.setConf(conf);
        augmentGraphWithDegreesJob.run(new String[] {
            "--input",
            augmentInputPath.toString(),
            "--output",
            augmentOutputPath.toString(),
            "--tempDir",
            new Path(tempDirPath, String.valueOf(System.currentTimeMillis()))
                .toString(), });

        /*
         * Enumerate triangles in the graph
         */
        Path enumerateInputPath = augmentOutputPath;
        // scatter the edges to lower degree vertex and build open triads
        Path enumerateOutputPath = new Path(tempDirPath, "enumerate"
            + String.valueOf(System.currentTimeMillis()));

        EnumerateTrianglesJob enumerateTrianglesJob = new EnumerateTrianglesJob();
        enumerateTrianglesJob.setConf(conf);
        enumerateTrianglesJob.run(new String[] {
            "--input",
            enumerateInputPath.toString(),
            "--output",
            enumerateOutputPath.toString(),
            "--tempDir",
            new Path(tempDirPath, String.valueOf(System.currentTimeMillis()))
                .toString(), });

        /*
         * Drop edges with insufficient support
         */
        Path checkSupportInputPath = enumerateOutputPath;
        Path checkSupportOutputPath = new Path(tempDirPath, "support"
            + String.valueOf(System.currentTimeMillis()));
        Job checkTrianglesForSupport = prepareJob(checkSupportInputPath,
            checkSupportOutputPath, SequenceFileInputFormat.class,
            SplitTrianglesToEdgesMapper.class, UndirectedEdge.class,
            IntWritable.class, DropUnsupportedEdgesReducer.class,
            UndirectedEdge.class, NullWritable.class,
            SequenceFileOutputFormat.class);

        checkTrianglesForSupport.setCombinerClass(IntSumReducer.class);
        checkTrianglesForSupport.getConfiguration().setInt(K, k);
        checkTrianglesForSupport.waitForCompletion(true);

        currentTrussesDirPath = checkSupportOutputPath;

        long droppedEdges = checkTrianglesForSupport.getCounters()
            .findCounter(Counter.DROPPED_EDGES).getValue();
        log.info("{} edges were dropped", droppedEdges);
        if (droppedEdges == 0L) {
          break;
        }

      }
    }

    Path componentsInputPath = new Path(tempDirPath, "converted"
        + String.valueOf(System.currentTimeMillis()));
    if (shouldRunNextPhase(parsedArgs, currentPhase)) {
      /*
       * Prepare the input for FindComponents
       */
      Job convertFromat = prepareJob(currentTrussesDirPath, componentsInputPath,
          SequenceFileInputFormat.class, PrepareInputMapper.class,
          Vertex.class, FlaggedVertex.class, Reducer.class, Vertex.class,
          FlaggedVertex.class, SequenceFileOutputFormat.class);
      convertFromat.waitForCompletion(true);
    }

    if (shouldRunNextPhase(parsedArgs, currentPhase)) {
      /*
       * Find the components of the remaining graph
       */
      FindComponentsJob componentsJob = new FindComponentsJob();
      componentsJob.setConf(conf);
      componentsJob.run(new String[] { "--input",
          componentsInputPath.toString(), "--output", outputPath.toString(),
          "--tempDir", tempDirPath.toString(), });
    }
    return 0;
  }

  private static final IntWritable ONE = new IntWritable(1);

  /**
   * Constant to access the configuration for parameter <code>k</code>.
   */
  public final static String K = "K";

  private static Logger log = LoggerFactory.getLogger(FindKTrussesJob.class);

  /**
   * Reading a triangle file, emit a record for each edge involved in each
   * triangle.
   */
  public static class SplitTrianglesToEdgesMapper extends
      Mapper<Triangle, Object, UndirectedEdge, IntWritable> {

    @Override
    public void map(Triangle triangle, Object obj, Context ctx)
        throws IOException, InterruptedException {
      ctx.write(
          new UndirectedEdge(triangle.getFirstVertex(), triangle
              .getSecondVertex()), ONE);
      ctx.write(
          new UndirectedEdge(triangle.getFirstVertex(), triangle
              .getThirdVertex()), ONE);
      ctx.write(
          new UndirectedEdge(triangle.getSecondVertex(), triangle
              .getThirdVertex()), ONE);
    }
  }

  /**
   * Keeps only the edges with sufficient support.
   */
  public static class DropUnsupportedEdgesReducer extends
      Reducer<UndirectedEdge, IntWritable, UndirectedEdge, NullWritable> {

    /**
     * The parameter <code>k</code> of the algorithm.
     */
    private int k;

    @Override
    public void setup(Context ctx) {
      k = ctx.getConfiguration().getInt(K, 3);
    }

    @Override
    public void reduce(UndirectedEdge edge, Iterable<IntWritable> counts,
        Context ctx) throws IOException, InterruptedException {
      int issupported = 0;
      for (IntWritable count : counts) {
        issupported += count.get();
        if (issupported >= k - 2) {
          break;// lazy evaluation
        }
      }
      if (issupported >= k - 2) {

        log.trace("supported {} (k is {})", edge, k);
        // ctx.write(edge.getFirstVertex(),
        // FlaggedVertex.createUndirectedEdge(edge.getSecondVertex()));
        ctx.write(edge, NullWritable.get());
      } else {
        log.trace("dropping {} (k is {})", edge, k);
        ctx.getCounter(Counter.DROPPED_EDGES).increment(1L);
      }
    }
  }
}
