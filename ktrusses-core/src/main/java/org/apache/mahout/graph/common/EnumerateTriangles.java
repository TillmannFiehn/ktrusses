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

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.graph.model.GeneralGraphElement;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.OpenTriad;
import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.Triangle;
import org.apache.mahout.graph.model.Vertex;
import org.apache.mahout.graph.model.VertexWithDegree;

/**
 * Container for the {@link EnumerateTrianglesJob } mapper and reducer classes.
 */
public class EnumerateTriangles {

  /**
   * Finds the lower degree vertex of an edge and emits key-value-pairs to bin
   * under this lower degree vertex.
   */
  public static class ScatterEdgesToLowerDegreeVertex extends
      Mapper<Object, GeneralGraphElement, Membership, GeneralGraphElement> {

    @Override
    public void map(Object key, GeneralGraphElement value, Context ctx)
        throws IOException, InterruptedException {

      Set<VertexWithDegree> order = TotalVertexOrder.getOrdered((RepresentativeEdge) value.getValue());
      VertexWithDegree lower = order.iterator().next();
      if (lower.getDegree() > 1) {
        ctx.write(new Membership().addMember(lower.getVertex()), value);
      }
    }
  }

  /**
   * Builds open triads from edges by pairwise joining the edges on the lower
   * degree vertex which is the apex of the triad. Emits key-value pairs where
   * the value is the triad and the key is the two outside vertices.
   * 
   */
  public static class BuildOpenTriads extends
      Reducer<Membership, GeneralGraphElement, Membership, GeneralGraphElement> {

    @Override
    public void reduce(Membership key, Iterable<GeneralGraphElement> values,
        Context ctx) throws IOException, InterruptedException {

      Vertex lower = key.getMembers().iterator().next();

      for (GeneralGraphElement generalouter : values) { // nested loop join
        RepresentativeEdge outer = (RepresentativeEdge) generalouter.getValue();
        for (GeneralGraphElement generalinner : values) {
          RepresentativeEdge inner = (RepresentativeEdge) generalinner.getValue();
          if (!outer.equals(inner)) {
            Set<VertexWithDegree> outerSet = TotalVertexOrder.getOrdered(outer);
            Set<VertexWithDegree> innerSet = TotalVertexOrder.getOrdered(inner);
            outerSet.remove(lower); // VwD equals V if vertices equal
            innerSet.remove(lower); // VwD equals V if vertices equal
            // build the new key
            Membership newkey = new Membership();
            newkey.addMember(outerSet.iterator().next().getVertex());
            newkey.addMember(innerSet.iterator().next().getVertex());

            OpenTriad newvalue = new OpenTriad();
            newvalue.setApex(lower);
            newvalue.addEdge(outer);
            newvalue.addEdge(inner);
            ctx.write(newkey, new GeneralGraphElement(newvalue));
          }
        }
      }
    }
  }

  /**
   * Joins {@link RepresentativeEdge } and {@link OpenTriad} on the outside
   * vertices of the triad.
   */
  public static class BuildTriangles extends
      Reducer<Membership, GeneralGraphElement, Membership, GeneralGraphElement> {

    @Override
    public void reduce(Membership key, Iterable<GeneralGraphElement> values, Context ctx)
        throws IOException, InterruptedException {
      Set<RepresentativeEdge> edges = new TreeSet<RepresentativeEdge>();
      Set<OpenTriad> triads = new TreeSet<OpenTriad>();
      // TODO avoid NLJ via a smart merging and partitioning of input keys
      for (GeneralGraphElement general : values) { // build sets with separate inputs
        @SuppressWarnings("rawtypes")
        WritableComparable value = general.getValue();
        if (value instanceof OpenTriad) {
          triads.add(OpenTriad.duplicate((OpenTriad) value));
        }
        if (value instanceof RepresentativeEdge) {
          edges.add(RepresentativeEdge.duplicate((RepresentativeEdge) value));
        }
      }
      for (OpenTriad triad : triads) { // nested loop join
        for (RepresentativeEdge edge : edges) {
          Triangle triangle = new Triangle();
          triangle.getEdges().addAll(triad.getEdges());
          triangle.addEdge(edge);
          Membership m = new Membership().addMember(triad.getApex());
          m.addMember(edge.getVertex0());
          m.addMember(edge.getVertex1());
          ctx.write(m, new GeneralGraphElement(triangle));
        }
      }
    }
  }
}
