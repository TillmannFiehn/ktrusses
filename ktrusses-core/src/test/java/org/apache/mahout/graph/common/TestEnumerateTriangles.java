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
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.Parser;
import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.SimpleParser;
import org.apache.mahout.graph.model.Vertex;
import org.junit.Test;

import com.google.common.io.Resources;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.mahout.graph.model.Triangle;

public class TestEnumerateTriangles extends MahoutTestCase {

  @Test
  public void testEnumerateTrianglesJob() throws Exception {

    File inputFile = new File(Resources.getResource("trianglestest.csv").toURI());
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

    EnumerateTrianglesJob enumerateJob = new EnumerateTrianglesJob();
    enumerateJob.setConf(conf);
    assertTrue(intermedediateFile.canRead());

    outputDir = getTestTempDir("trianglestest-out");
    tempDir = getTestTempDir("trianglestest-tmp");
    outputDir.delete();
    tempDir.delete();

    enumerateJob.run(new String[] { "--input",
        intermedediateFile.getAbsolutePath(), "--output",
        outputDir.getAbsolutePath(), "--tempDir", tempDir.getAbsolutePath() });

    Path output = new Path(
        new File(outputDir, "part-r-00000").getAbsolutePath());

    FileStatus outputStat = sys.getFileStatus(output);

    HashSet<Triangle> triangles = getTestFileContents(
        inputFile, sys, conf);
    FileSplit s = new FileSplit(output, 0L, outputStat.getLen(), new String[0]);
    SequenceFileRecordReader<Membership, RepresentativeEdge> r = new SequenceFileRecordReader<Membership, RepresentativeEdge>();
    r.initialize(s, new TaskAttemptContext(conf, new TaskAttemptID()));
    /*while (r.nextKeyValue()) {
      Membership m = r.getCurrentKey();
      RepresentativeEdge e = r.getCurrentValue();
      System.out.println(String.format(
          "Job returned %s binned under membership %s. Testing map...", e, m));
      RepresentativeEdge test = edges.remove(m);
      assertEquals(test, e);
	  assertEquals(test.)

      assertEquals(test.getDegree(test.getVertex0()),
          e.getDegree(e.getVertex0()));
      assertEquals(test.getDegree(test.getVertex1()),
          e.getDegree(e.getVertex1()));
    }
    assertTrue(edges.isEmpty());*/
  }

  private HashSet<Triangle> getTestFileContents(
      File file, FileSystem sys, Configuration conf) throws IOException {
    Path path = new Path(file.getAbsolutePath());
    FileStatus stat = sys.getFileStatus(path);
    FileSplit s = new FileSplit(path, 0L, stat.getLen(), new String[0]);
    Parser parser = new SimpleParser();
    HashMap<Membership, RepresentativeEdge> edges = new HashMap<Membership, RepresentativeEdge>();
    HashMap<Vertex,Membership> vertexes = new HashMap<Vertex,Membership>();
    LineRecordReader l = new LineRecordReader();
    l.initialize(s, new TaskAttemptContext(conf, new TaskAttemptID()));
    while (l.nextKeyValue()) {
      Text t = l.getCurrentValue();
      Vector<Vertex> members = parser.parse(t);
      if (members != null && members.size() > 1) {
        for (Vertex vertex : members) {
          Membership neighbours = vertexes.containsKey(vertex) ? 
            vertexes.get(vertex) : new Membership();
          for (Vertex v : members) if (!v.equals(vertex)) neighbours.addMember(v);
          vertexes.put(vertex, neighbours);
        }
      }
    }
    HashSet<Triangle> triangles = new HashSet<Triangle>();
    HashSet<Vertex> visited = new HashSet<Vertex>();
    for (Membership m : edges.keySet()) {
      for (Vertex v : m.getMembers()) {
        
      }
    }
  return triangles;
  }
}
