/**
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.DummyRecordWriter;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.graph.common.SimplifyGraph.SimplifyGraphMapper;
import org.apache.mahout.graph.common.SimplifyGraph.SimplifyGraphReducer;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.Parser;
import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.SimpleParser;
import org.apache.mahout.graph.model.Vertex;
import org.junit.Test;

public class TestSimplifyGraph extends MahoutTestCase {

  @Test
  public void testSimplifyGraphMapper() {

    try {
      Configuration conf = new Configuration();
      conf.set(Parser.class.getCanonicalName(), SimpleParser.class.getName());
      DummyRecordWriter<Membership, RepresentativeEdge> writer =
              new DummyRecordWriter<Membership, RepresentativeEdge>();

      SimplifyGraphMapper simplifier = new SimplifyGraphMapper();

      SimplifyGraphMapper.Context ctx = DummyRecordWriter.build(simplifier,
              conf, writer);
      simplifier.setup(ctx);

      String[] file = new String[]{"1\t1", "1\t2", "2\t1", "2\t2",};

      for (String line : file) {
        simplifier.map(null, new Text(line), ctx);
      }

      Map<Membership, List<RepresentativeEdge>> output = writer.getData();

      assertEquals(output.size(), 1);

      Membership key = new Membership();
      key.addMember(new Vertex(1L));
      key.addMember(new Vertex(2L));

      List<RepresentativeEdge> edges = output.get(key);

      assertNotNull(edges);

      assertEquals(edges.size(), 2);

      RepresentativeEdge e = new RepresentativeEdge(new Vertex(1L), new Vertex(
              2L));

      assertTrue(edges.remove(e));
      assertTrue(edges.remove(e));

    } catch (IOException e) {
      throw new RuntimeException();
    } catch (InterruptedException e) {
      throw new RuntimeException();
    }
  }

  @Test
  public void testSimplifyGraphReducer() {

    try {
      Configuration conf = new Configuration();
      conf.set(Parser.class.getCanonicalName(), SimpleParser.class.getName());
      DummyRecordWriter<Membership, RepresentativeEdge> writer =
              new DummyRecordWriter<Membership, RepresentativeEdge>();

      SimplifyGraphMapper simplifier = new SimplifyGraphMapper();

      SimplifyGraphMapper.Context ctxm = DummyRecordWriter.build(simplifier,
              conf, writer);
      simplifier.setup(ctxm);

      String[] file = new String[]{"1\t1", "1\t2", "2\t1", "2\t2",};

      for (String line : file) {
        simplifier.map(null, new Text(line), ctxm);
      }

      Map<Membership, List<RepresentativeEdge>> output = writer.getData();

      SimplifyGraphReducer aggregator = new SimplifyGraphReducer();

      writer = new DummyRecordWriter<Membership, RepresentativeEdge>();

      SimplifyGraphReducer.Context ctxr = DummyRecordWriter.build(aggregator,
              conf, writer, Membership.class, RepresentativeEdge.class);

      for (Entry<Membership, List<RepresentativeEdge>> entry : output.entrySet()) {

        aggregator.reduce(entry.getKey(), entry.getValue(), ctxr);

      }

      output = writer.getData();

      assertEquals(output.size(), 1);

      Membership key = new Membership();
      key.addMember(new Vertex(1L));
      key.addMember(new Vertex(2L));

      List<RepresentativeEdge> edges = output.get(key);

      assertNotNull(edges);

      assertEquals(edges.size(), 1);

      RepresentativeEdge e = new RepresentativeEdge(new Vertex(1L), new Vertex(
              2L));

      assertTrue(edges.remove(e));
      assertFalse(edges.remove(e));

    } catch (IOException e) {
      throw new RuntimeException();
    } catch (InterruptedException e) {
      throw new RuntimeException();
    }

  }
}
