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

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.OpenTriad;
import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.Triangle;
import org.apache.mahout.graph.model.Vertex;
import org.apache.mahout.graph.model.VertexWithDegree;

public class EnumerateTriangles {

  public static class ScatterEdgesToLowerDegreeVertex extends
          Mapper<Object, RepresentativeEdge, Membership, RepresentativeEdge> {

    @Override
    public void map(Object key, RepresentativeEdge value, Context ctx)
            throws IOException, InterruptedException {

      Set<VertexWithDegree> order = TotalVertexOrder.getOrdered(value);
      VertexWithDegree lower = order.iterator().next();
      if (lower.getDegree() > 1) {
        ctx.write(new Membership().addMember(lower.getVertex()), value);
      }
    }
  }

  public static class BuildOpenTriads extends
          Reducer<Membership, RepresentativeEdge, Membership, OpenTriad> {

    @Override
    public void reduce(Membership key, Iterable<RepresentativeEdge> values,
            Context ctx) throws IOException, InterruptedException {

      Vertex lower = key.getMembers().iterator().next();

      for (RepresentativeEdge outer : values) { // nested loop join
        for (RepresentativeEdge inner : values) {
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
            ctx.write(newkey, newvalue);
          }
        }
      }
    }
  }

  public static class BuildTriangles extends
          Reducer<Membership, Object, Membership, Triangle> {

    @Override
    public void reduce(Membership key, Iterable<Object> values,
            Context ctx) throws IOException, InterruptedException {
      Set<RepresentativeEdge> edges = new TreeSet<RepresentativeEdge>();
      Set<OpenTriad> triads = new TreeSet<OpenTriad>();
      for (Object value : values) { // build sets with sperate inputs
        if (value instanceof OpenTriad) {
          triads.add((OpenTriad) value);
        }
        if (value instanceof RepresentativeEdge) {
          edges.add((RepresentativeEdge) value);
        }
      }
      for (OpenTriad triad : triads) { //nested loop join
        for (RepresentativeEdge edge : edges) {
          //TODO implement the triangles
        }
      }
    }
  }
}
