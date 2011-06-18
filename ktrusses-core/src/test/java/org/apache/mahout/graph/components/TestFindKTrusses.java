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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.mahout.common.DummyRecordWriter;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.graph.common.EnumerateTrianglesJob;
import org.apache.mahout.graph.components.FindKTrusses.SplitTrianglesToEdges;
import org.apache.mahout.graph.model.GenericGraphElement;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.Triangle;
import org.apache.mahout.graph.model.Vertex;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestFindKTrusses extends MahoutTestCase {

  @Test
  public void testSplitTrianglesToEdges() {

    try {

      DummyRecordWriter<Membership, GenericGraphElement> writer = new DummyRecordWriter<Membership, GenericGraphElement>();

      SplitTrianglesToEdges splitter = new SplitTrianglesToEdges();

      SplitTrianglesToEdges.Context ctx = DummyRecordWriter.build(splitter,
          new Configuration(), writer);

      Triangle[] file = new Triangle[] { factorize(1, 2, 3),
          factorize(2, 3, 4), };

      for (Triangle record : file) {
        splitter.map(Membership.factorize(record), new GenericGraphElement(
            record), ctx);
      }

      Map<Membership, List<GenericGraphElement>> output = writer.getData();

      assertEquals(5, output.size());

      for (Membership m : new Membership[] {
          Membership.factorize(factorize(1, 2)),
          Membership.factorize(factorize(1, 3)),
          Membership.factorize(factorize(2, 3)),

          Membership.factorize(factorize(2, 3)),
          Membership.factorize(factorize(2, 4)),
          Membership.factorize(factorize(3, 4)), }) {
        List<GenericGraphElement> edges = output.get(m);
        Iterator<Vertex> members = m.getMembers().iterator();
        long id0 = members.next().getId();
        long id1 = members.next().getId();

        assertTrue(edges.remove(new GenericGraphElement(factorize(id0, id1)))
            || edges.remove(new GenericGraphElement(factorize(id1, id0))));
        if (edges.size() == 0) {
          output.remove(m);
        }

      }
      assertTrue(output.isEmpty());

    } catch (IOException e) {
      throw new RuntimeException();
    } catch (InterruptedException e) {
      throw new RuntimeException();
    }
  }

  @Test
  public void testFindKTrussesJob() throws Exception {

    File inputFile = new File(Resources.getResource("ktrussestest.csv").toURI());
    assertTrue(inputFile.canRead());
    File ktrussesDir = getTestTempDir("ktrusses-out");
    File tempDir = getTestTempDir("ktrusses-tmp");

    ktrussesDir.delete();
    tempDir.delete();

    Configuration conf = new Configuration();
    FindKTrussesJob ktrussesJob = new FindKTrussesJob();
    ktrussesJob.setConf(conf);
    ktrussesJob.run(new String[] { "--input", inputFile.getAbsolutePath(),
        "--output", ktrussesDir.getAbsolutePath(), "--tempDir",
        tempDir.getAbsolutePath(), "--k", Integer.toString(4) });

    FileSystem sys = FileSystem.get(conf);
    Path output = new Path(
        new File(ktrussesDir, "part-r-00000").getAbsolutePath());
    FileStatus outputStat = sys.getFileStatus(output);

    FileSplit s = new FileSplit(output, 0L, outputStat.getLen(), new String[0]);
    SequenceFileRecordReader<Membership, GenericGraphElement> r = new SequenceFileRecordReader<Membership, GenericGraphElement>();
    r.initialize(s, new TaskAttemptContext(conf, new TaskAttemptID()));
    while (r.nextKeyValue()) {
      Membership m = r.getCurrentKey();
      RepresentativeEdge e = (RepresentativeEdge) r.getCurrentValue()
          .getValue();
      System.out.println(String.format(
          "Job returned %s binned under membership %s. Testing map...", e, m));
      // assertEquals(edges.remove(m), e);
    }

    // assertTrue(edges.isEmpty());
    
    File trianglesDir = getTestTempDir("triangles-out");

    trianglesDir.delete();
    tempDir.delete();

    EnumerateTrianglesJob enumerateTriangles = new EnumerateTrianglesJob();
    enumerateTriangles.setConf(conf);
    enumerateTriangles.run(new String[] { "--input", ktrussesDir.getAbsolutePath(),
        "--output", trianglesDir.getAbsolutePath(), "--tempDir",
        tempDir.getAbsolutePath() });

    output = new Path(
        new File(trianglesDir, "part-r-00000").getAbsolutePath());
    outputStat = sys.getFileStatus(output);

    s = new FileSplit(output, 0L, outputStat.getLen(), new String[0]);
    r.initialize(s, new TaskAttemptContext(conf, new TaskAttemptID()));
    while (r.nextKeyValue()) {
      Membership m = r.getCurrentKey();
      Triangle e = (Triangle) r.getCurrentValue()
          .getValue();
      System.out.println(String.format(
          "Job returned %s binned under membership %s. Testing map...", e, m));
    }
  }

  private static Triangle factorize(long v0, long v1, long v2) {
    Triangle t = new Triangle();
    t.addEdge(factorize(v0, v1));
    t.addEdge(factorize(v0, v2));
    t.addEdge(factorize(v1, v2));
    return t;
  }

  private static RepresentativeEdge factorize(long v0, long v1) {
    RepresentativeEdge e = new RepresentativeEdge();
    e.setVertex0(new Vertex(v0));
    e.setVertex1(new Vertex(v1));
    return e;
  }
}
