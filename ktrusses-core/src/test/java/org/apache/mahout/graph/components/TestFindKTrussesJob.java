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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.graph.model.Triangle;
import org.apache.mahout.graph.model.UndirectedEdge;
import org.apache.mahout.graph.model.Vertex;
import org.easymock.EasyMock;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestFindKTrussesJob extends MahoutTestCase {

  private static final IntWritable ONE = new IntWritable(1);

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testSplitTrianglesToEdges() throws Exception {

    Mapper.Context ctx = EasyMock.createMock(Mapper.Context.class);

    ctx.write(new UndirectedEdge(1, 2), ONE);
    ctx.write(new UndirectedEdge(1, 3), ONE);
//    ctx.write(new UndirectedEdge(2, 3), new IntWritable(2));
    ctx.write(new UndirectedEdge(2, 3), ONE);
    ctx.write(new UndirectedEdge(2, 3), ONE);
    ctx.write(new UndirectedEdge(2, 4), ONE);
    ctx.write(new UndirectedEdge(3, 4), ONE);

    EasyMock.replay(ctx);

    new FindKTrussesJob.SplitTrianglesToEdgesMapper().map(new Triangle(1, 2, 3),
        NullWritable.get(), ctx);
    new FindKTrussesJob.SplitTrianglesToEdgesMapper().map(new Triangle(2, 3, 4),
        NullWritable.get(), ctx);
    
    EasyMock.verify(ctx);
  }


  @Test
  public void toyIntegrationTest() throws Exception {
    File inputFile = getTestTempFile("graph.txt");
    File outputDir = getTestTempDir("output");
    outputDir.delete();
    File tempDir = getTestTempDir("tmp");

    writeLines(inputFile,
        "0,0",
        "0,1",
        "0,2",
        "0,3",
        "0,4",
        "0,5",
        "0,6",
        "0,7",
        "1,2",
        "1,3",
        "2,3",
        "4,5",
        "4,7");

    Configuration conf = new Configuration();
    FindKTrussesJob trussesJob = new FindKTrussesJob();
    trussesJob.setConf(conf);
    trussesJob.run(new String[] { "--input", inputFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath(),
        "--tempDir", tempDir.getAbsolutePath(), "--k", "4" });

    Set<UndirectedEdge> trusses = Sets.newHashSet();
    for (Pair<Vertex,VertexAndZone> result :
        new SequenceFileIterable<Vertex, VertexAndZone>(new Path(outputDir.getAbsolutePath() + "/part-r-00000"),
        false, conf)) {
      trusses.add(new UndirectedEdge(result.getFirst(), result.getSecond().getVertex()));
    }

    assertEquals(11, trusses.size());
    for(UndirectedEdge truss: trusses) {
      System.out.println(truss);
    }

  }


}
