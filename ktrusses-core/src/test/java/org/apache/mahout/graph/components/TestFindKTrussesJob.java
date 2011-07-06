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
import java.util.Iterator;
import java.util.Map;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TestFindKTrussesJob extends MahoutTestCase {

  @Before
  public void init() {
    Logger logger = LoggerFactory.getLogger(getClass());
    logger.trace("Hello I am running.");
  }
  
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
        "1,2",
        "0,3",
        "1,3",
        "2,3",
        "0,4",
        "0,5",
        "4,5",
        "0,7",
        "4,7",
        "0,6"
    );

    Configuration conf = new Configuration();
    FindKTrussesJob trussesJob = new FindKTrussesJob();
    trussesJob.setConf(conf);
    trussesJob.run(new String[] { "--input", inputFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath(),
        "--tempDir", tempDir.getAbsolutePath(), "--k", "4" });

    Map<Vertex, Set<Vertex>> components = Maps.newHashMap();
    for (Pair<Vertex, FlaggedVertex> result : new SequenceFileIterable<Vertex, FlaggedVertex>(
        new Path(outputDir.getAbsolutePath() + "/part-r-00000"), false, conf)) {
      Set<Vertex> component = components.get(result.getSecond().getVertex());
      if (component == null) {
        component = Sets.newTreeSet();
        components.put(result.getSecond().getVertex(), component);
      }
      component.add(result.getFirst());

    }

    assertEquals(1, components.size());
    assertEquals(Sets.newTreeSet(new Iterable<Vertex>() {
      Long[] comp = new Long[] { 0L, 1L, 2L, 3L, };

      @Override
      public Iterator<Vertex> iterator() {
        return new Iterator<Vertex>() {
          private int i = 0;

          @Override
          public boolean hasNext() {
            return i < comp.length;
          }

          @Override
          public Vertex next() {
            return new Vertex(comp[i++]);
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

        };
      }

    }), components.get(new Vertex(0L)));

  }


}
