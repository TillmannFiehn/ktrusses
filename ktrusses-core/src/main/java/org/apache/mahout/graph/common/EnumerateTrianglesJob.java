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
import org.apache.mahout.graph.common.EnumerateTriangles.BuildOpenTriads;
import org.apache.mahout.graph.common.EnumerateTriangles.ScatterEdgesToLowerDegreeVertex;
import org.apache.mahout.graph.model.GeneralGraphElement;
import org.apache.mahout.graph.model.Membership;

/**
 * Enumerates the triangles of a graph.
 */
public class EnumerateTrianglesJob extends AbstractJob {

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new EnumerateTrianglesJob(), args);
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
    Path triadsPath = new Path(tempDirPath, "triangles-" + System.currentTimeMillis());
    Path outputPath = getOutputPath();

    // scatter the edges to lower degree vertex and build open triads
    Job scatter = prepareJob(inputPath,
            triadsPath,
            SequenceFileInputFormat.class,
            ScatterEdgesToLowerDegreeVertex.class,
            Membership.class,
            GeneralGraphElement.class,
            BuildOpenTriads.class,
            Membership.class,
            GeneralGraphElement.class,
            SequenceFileOutputFormat.class);

    scatter.waitForCompletion(true);
    
    //join triads and edges pairwise to get all triangles
    Job join = prepareJob(new Path(triadsPath + "," + inputPath),
            outputPath,
            SequenceFileInputFormat.class,
            Mapper.class,
            Membership.class,
            GeneralGraphElement.class,
            JoinDegrees.class,
            Membership.class,
            GeneralGraphElement.class,
            SequenceFileOutputFormat.class);


    join.waitForCompletion(true);
    
    
    
    return 0;
  }
  
}
