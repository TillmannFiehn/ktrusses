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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.graph.model.GeneralGraphElement;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.Parser;
import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.SimpleParser;
import org.apache.mahout.graph.model.Vertex;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestAugmentGraphWithDegrees extends MahoutTestCase {

  @Before
  public void logLevel() {
    Logger.getLogger("org.apache.mahout.graph").setLevel(Level.TRACE);
  }

  @Test
  public void testAugmentGraphWithDegreesGraphJob() throws Exception {

    // run simplification first
    File inputFile = new File(Resources.getResource("augmenttest.csv").toURI());
    assertTrue(inputFile.canRead());
    File outputDir = getTestTempDir("simplifytest-out");
    File tempDir = getTestTempDir("simplifytest-tmp");
    outputDir.delete();
    tempDir.delete();
    Configuration conf = new Configuration();
    SimplifyGraphJob simplifyGraphJob = new SimplifyGraphJob();
    simplifyGraphJob.setConf(conf);
    simplifyGraphJob.run(new String[] { "--input", inputFile.getAbsolutePath(),
        "--output", outputDir.getAbsolutePath(), "--tempDir",
        tempDir.getAbsolutePath() });

    FileSystem sys = FileSystem.get(conf);

    File intermedediateFile = new File(outputDir, "part-r-00000");

    // augment the simplified graph
    AugmentGraphWithDegreesJob augmentJob = new AugmentGraphWithDegreesJob();
    augmentJob.setConf(conf);
    assertTrue(intermedediateFile.canRead());

    outputDir = getTestTempDir("augmenttest-out");
    tempDir = getTestTempDir("augmenttest-tmp");
    outputDir.delete();
    tempDir.delete();

    augmentJob.run(new String[] { "--input",
        intermedediateFile.getAbsolutePath(), "--output",
        outputDir.getAbsolutePath(), "--tempDir", tempDir.getAbsolutePath() });

    Path output = new Path(
        new File(outputDir, "part-r-00000").getAbsolutePath());

    FileStatus outputStat = sys.getFileStatus(output);

    HashMap<Membership, RepresentativeEdge> edges = getTestFileContents(
        inputFile, sys, conf);
    FileSplit s = new FileSplit(output, 0L, outputStat.getLen(), new String[0]);
    SequenceFileRecordReader<Membership, GeneralGraphElement> r = new SequenceFileRecordReader<Membership, GeneralGraphElement>();
    r.initialize(s, new TaskAttemptContext(conf, new TaskAttemptID()));
    while (r.nextKeyValue()) {
      Membership m = r.getCurrentKey();
      RepresentativeEdge e = (RepresentativeEdge) r.getCurrentValue().getValue();
      System.out.println(String.format(
          "Job returned %s binned under membership %s. Testing map...", e, m));
      RepresentativeEdge test = edges.remove(m);
      assertEquals(test, e);
      assertEquals(test.getDegree(test.getVertex0()),
          e.getDegree(e.getVertex0()));
      assertEquals(test.getDegree(test.getVertex1()),
          e.getDegree(e.getVertex1()));
    }
    assertTrue(edges.isEmpty());
  }

  private HashMap<Membership, RepresentativeEdge> getTestFileContents(
      File file, FileSystem sys, Configuration conf) throws IOException {
    Path path = new Path(file.getAbsolutePath());
    FileStatus stat = sys.getFileStatus(path);
    FileSplit s = new FileSplit(path, 0L, stat.getLen(), new String[0]);
    Parser parser = new SimpleParser();
    HashMap<Membership, RepresentativeEdge> edges = new HashMap<Membership, RepresentativeEdge>();
    LineRecordReader l = new LineRecordReader();
    l.initialize(s, new TaskAttemptContext(conf, new TaskAttemptID()));
    while (l.nextKeyValue()) {
      Text t = l.getCurrentValue();
      Vector<Vertex> members = parser.parse(t);
      if (members != null && members.size() > 1)
        edges.put(new Membership().setMembers(members), new RepresentativeEdge(
            members.get(0), members.get(1)));
    }
    HashMap<Long, Long> degrees = new HashMap<Long, Long>();
    for (RepresentativeEdge edge : edges.values()) {
      Vertex v0 = edge.getVertex0();
      Long d0 = degrees.get(v0.getId());
      if (d0 == null || d0 == 0) {
        d0 = 1L;
      } else {
        d0 += 1L;
      }
      degrees.put(v0.getId(), d0);
      Vertex v1 = edge.getVertex1();
      Long d1 = degrees.get(v1.getId());
      if (d1 == null || d1 == 0) {
        d1 = 1L;
      } else {
        d1 += 1L;
      }
      degrees.put(v1.getId(), d1);
    }
    for (RepresentativeEdge edge : edges.values()) {
      Vertex v0 = edge.getVertex0();
      edge.setDegree(v0, degrees.get(v0.getId()));
      Vertex v1 = edge.getVertex1();
      edge.setDegree(v1, degrees.get(v1.getId()));
    }
    return edges;
  }
}
