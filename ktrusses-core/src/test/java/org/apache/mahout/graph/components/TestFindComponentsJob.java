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
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.graph.model.Vertex;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TestFindComponentsJob extends MahoutTestCase {

  @Test
  public void toyIntegrationTest() throws Exception {
    File inputFile = getTestTempFile("graph.txt");
    File outputDir = getTestTempDir("output");
    outputDir.delete();
    File tempDir = getTestTempDir("tmp");

    writeLines(inputFile, "0,0", "0,1", "0,2", "0,3", "0,4", "0,5", "0,6",
        "0,7", "1,2", "1,3", "2,3", "4,5", "4,7", "8,9", "9,10");

    Configuration conf = new Configuration();

    File simplifyOutputPath = new File(tempDir.getAbsolutePath() + "/simplify"
        + System.currentTimeMillis());

    /*
     * Simplify the graph first
     */
    PrepareInputJob prepareInputJob = new PrepareInputJob();
    prepareInputJob.setConf(conf);
    prepareInputJob.run(new String[] { "--input", inputFile.getAbsolutePath(),
        "--output", simplifyOutputPath.getAbsolutePath(), "--tempDir",
        tempDir.getAbsolutePath(), });

    FindComponentsJob componentsJob = new FindComponentsJob();
    componentsJob.setConf(conf);
    componentsJob.run(new String[] { "--input",
        simplifyOutputPath.getAbsolutePath(), "--output",
        outputDir.getAbsolutePath(), "--tempDir", tempDir.getAbsolutePath(), });

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

    assertEquals(2, components.size());
//    System.out.println(components);
    assertEquals(Sets.newTreeSet(new Iterable<Vertex>() {
      Long[] comp = new Long[] { 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, };

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
    assertEquals(Sets.newTreeSet(new Iterable<Vertex>() {
      Long[] comp = new Long[] { 8L, 9L, 10L, };

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

    }), components.get(new Vertex(8L)));
  }
}
