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

package org.apache.mahout.graph.common;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.graph.common.AugmentGraphWithDegrees.JoinDegrees;
import org.apache.mahout.graph.common.AugmentGraphWithDegrees.ScatterEdges;
import org.apache.mahout.graph.common.AugmentGraphWithDegrees.SumDegrees;
import org.apache.mahout.graph.model.GenericGraphElement;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.RepresentativeEdge;

/**
 * Augments a graph with degree information for each vertex which is the number
 * of {@link RepresentativeEdge}s that point to or from this very vertex.
 */
public class AugmentGraphWithDegreesJob extends AbstractJob {

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new AugmentGraphWithDegreesJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    addInputOption();
    addOutputOption();

    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }

    Path tempDirPath = new Path(parsedArgs.get("--tempDir"));

    Path inputPath = getInputPath();
    Path augmentedEdgesPath = new Path(tempDirPath, "augmented-edges-"
        + System.currentTimeMillis());
    Path outputPath = getOutputPath();

    // scatter the edges to each of the vertices and count degree
    Job scatter = prepareJob(inputPath, augmentedEdgesPath,
        SequenceFileInputFormat.class, ScatterEdges.class, Membership.class,
        RepresentativeEdge.class, SumDegrees.class, Membership.class,
        RepresentativeEdge.class, SequenceFileOutputFormat.class);

    scatter.waitForCompletion(true);

    // join augmented edges with partial degree information to to complete
    // records
    Job join = prepareJob(augmentedEdgesPath, outputPath,
        SequenceFileInputFormat.class, Mapper.class, Membership.class,
        RepresentativeEdge.class, JoinDegrees.class, Membership.class,
        GenericGraphElement.class, SequenceFileOutputFormat.class);

    join.waitForCompletion(true);

    return 0;
  }
}
