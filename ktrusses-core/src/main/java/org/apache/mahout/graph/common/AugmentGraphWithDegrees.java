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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.mahout.graph.model.GeneralGraphElement;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.Vertex;

/**
 * Container for the {@link AugmentGraphWithDegrees } mapper and reducer classes
 */
public class AugmentGraphWithDegrees {

  private static Logger log = Logger.getLogger(AugmentGraphWithDegrees.class);

  /**
   * Sends every edge to each vertices
   */
  public static class ScatterEdges extends
      Mapper<Object, RepresentativeEdge, Membership, RepresentativeEdge> {

    @Override
    public void map(Object key, RepresentativeEdge value, Context ctx)
        throws IOException, InterruptedException {

      ctx.write(new Membership().addMember(value.getVertex0()), RepresentativeEdge.duplicate(value));
      ctx.write(new Membership().addMember(value.getVertex1()), RepresentativeEdge.duplicate(value));

    }
  }

  /**
   * Sums up the count of edges for each vertex and augments all edges with a
   * degree information for the key vertex
   */
  public static class SumDegrees extends
      Reducer<Membership, RepresentativeEdge, Membership, RepresentativeEdge> {

    @Override
    public void reduce(Membership key, Iterable<RepresentativeEdge> values,
        Context ctx) throws IOException, InterruptedException {
      long degree = 0;
      List<RepresentativeEdge> edges = new LinkedList<RepresentativeEdge>();
      Iterator<RepresentativeEdge> i = values.iterator();
      while (i.hasNext()) {
        edges.add(RepresentativeEdge.duplicate(i.next()));
        degree++; // calculate degree
      }

      Vertex v = key.getMembers().iterator().next();

      Iterator<RepresentativeEdge> j = edges.iterator();
      while (j.hasNext()) {
        RepresentativeEdge edge = j.next();
        edge.setDegree(v, degree); // augment the edge
        Membership newkey = new Membership().addMember(edge.getVertex0())
            .addMember(edge.getVertex1());
        log.trace(String.format(
            "augmentet edge %s, binned under %s.",
            edge, newkey));
        ctx.write(newkey, edge);
      }

    }
  }

  /**
   * Joins identical edges assuring degree augmentations for both nodes
   */
  public static class JoinDegrees extends
      Reducer<Membership, RepresentativeEdge, Membership, GeneralGraphElement> {

    @SuppressWarnings("rawtypes")
    @Override
    public void reduce(Membership key, Iterable<RepresentativeEdge> values,
        Context ctx) throws IOException, InterruptedException {
      long d0 = Integer.MIN_VALUE;
      long d1 = Integer.MIN_VALUE;
      List<RepresentativeEdge> edges = new LinkedList<RepresentativeEdge>();
      for (RepresentativeEdge edge : values) {
        edge = RepresentativeEdge.duplicate(edge);
        edges.add(edge);
        if (d0 < 0) {
          Vertex v = edge.getVertex0();
          d0 = edge.getDegree(v);
        }

        if (d1 < 0) {
          Vertex v = edge.getVertex1();
          d1 = edge.getDegree(v);
        }

      }

      RepresentativeEdge edge = edges.iterator().next();

      Vertex v0 = edge.getVertex0();
      edge.setDegree(v0, d0);
      Vertex v1 = edge.getVertex1();
      edge.setDegree(v1, d1);

      log.trace(String.format(
          "fully augmentet edge %s, binned under %s.",
          edge, key));
      ctx.write(key, new GeneralGraphElement(edge));
    }
  }
}
