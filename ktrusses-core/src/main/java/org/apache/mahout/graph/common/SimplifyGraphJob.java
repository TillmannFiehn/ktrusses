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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.graph.common.SimplifyGraph.SimplifyGraphMapper;
import org.apache.mahout.graph.common.SimplifyGraph.SimplifyGraphReducer;
import org.apache.mahout.graph.model.GenericGraphElement;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.Parser;
import org.apache.mahout.graph.model.RepresentativeEdge;

/**
 * Simplifies a graph. That is: remove loops, aggregate edges to
 * {@link RepresentativeEdge }. The input file format is a
 * {@link TextInputFormat} which can be parsed via an implementation of
 * {@link Parser}.
 * 
 * This job accepts three input arguments
 * 
 * <pre>
 *  input
 *  output
 *  org.apache.mahout.graph.model.Parser
 * </pre>
 * 
 * The output is a {@link SequenceFile} containing a {@link Membership} as key
 * and a {@link RepresentativeEdge} as value.
 */
public class SimplifyGraphJob extends AbstractJob {

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new SimplifyGraphJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    addInputOption();
    addOutputOption();
    addOption(
        Parser.class.getCanonicalName(),
        Parser.class.getCanonicalName(),
        "A class implementing the Parser interface that should be used to parse the graph file.");

    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }

    String parserImplementationClass = parsedArgs.get("--"
        + Parser.class.getCanonicalName()); // extract parameter

    Path inputPath = getInputPath();
    Path outputPath = getOutputPath();

    Job simplify = prepareJob(inputPath, outputPath, TextInputFormat.class,
        SimplifyGraphMapper.class, Membership.class, GenericGraphElement.class,
        SimplifyGraphReducer.class, Membership.class, GenericGraphElement.class,
        SequenceFileOutputFormat.class);

    if (parserImplementationClass != null) { // pass parser parameter to the job
                                             // if set
      simplify.getConfiguration().set(Parser.class.getCanonicalName(),
          parserImplementationClass);
    }

    simplify.waitForCompletion(true);

    return 0;
  }
}
