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
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.graph.model.GenericGraphElement;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.OpenTriad;
import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.Triangle;
import org.apache.mahout.graph.model.VertexWithDegree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container for the {@link EnumerateTrianglesJob } mapper and reducer classes.
 */
public class EnumerateTriangles {

  private static Logger log = LoggerFactory.getLogger(EnumerateTriangles.class);

  /**
   * Finds the lower degree vertex of an edge and emits key-value-pairs to bin
   * under this lower degree vertex.
   */
  public static class ScatterEdgesToLowerDegreeVertex extends
      Mapper<Object, GenericGraphElement, Membership, GenericGraphElement> {

    @Override
    public void map(Object key, GenericGraphElement generic, Context ctx)
        throws IOException, InterruptedException {

      Set<VertexWithDegree> order = TotalVertexOrder
          .getOrdered((RepresentativeEdge) generic.getValue());
      VertexWithDegree lower = order.iterator().next();
      if (lower.getDegree() > 1) {
        log.trace(String.format("edge %s under lower degree vertex %s.",
            generic, lower));
        // build the new key of the lower vertex
        Membership newkey = new Membership().addMember(lower.getVertex());
        ctx.write(newkey, generic);
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
      Reducer<Membership, GenericGraphElement, Membership, GenericGraphElement> {

    @Override
    public void reduce(Membership key, Iterable<GenericGraphElement> generics,
        Context ctx) throws IOException, InterruptedException {

      List<RepresentativeEdge> map = new LinkedList<RepresentativeEdge>();

      for (GenericGraphElement generic : generics) { // nested loop join
        RepresentativeEdge probe = RepresentativeEdge
            .duplicate((RepresentativeEdge) generic.getValue());
        for (RepresentativeEdge build : map) {

          if (!probe.equals(build)) {
            Iterator<VertexWithDegree> iterator = TotalVertexOrder.getOrdered(
                build, probe).iterator();
            VertexWithDegree lower = iterator.next();
            iterator.remove();
            // build the new key of the outer vertices
            Membership newkey = new Membership();
            newkey.addMember(iterator.next().getVertex());
            newkey.addMember(iterator.next().getVertex());

            OpenTriad newvalue = new OpenTriad();
            newvalue.setApex(lower.getVertex());
            newvalue.addEdge(probe);
            newvalue.addEdge(build);
            log.trace(String.format("open triad under membership key %s.",
                newvalue, newkey));
            ctx.write(newkey, new GenericGraphElement(newvalue));
          }
        }
        map.add(probe);
      }
    }
  }

  /**
   * Joins {@link RepresentativeEdge } and {@link OpenTriad} on the outside
   * vertices of the triad.
   */
  public static class BuildTriangles extends
      Reducer<Membership, GenericGraphElement, Membership, GenericGraphElement> {

    @Override
    public void reduce(Membership key, Iterable<GenericGraphElement> generics,
        Context ctx) throws IOException, InterruptedException {
      Set<RepresentativeEdge> edges = new TreeSet<RepresentativeEdge>();
      Set<OpenTriad> triads = new TreeSet<OpenTriad>();
      // TODO avoid NLJ via a smart merging and partitioning of input keys
      for (GenericGraphElement generic : generics) { // build sets with separate
                                                     // inputs
        @SuppressWarnings("rawtypes")
        WritableComparable value = generic.getValue();
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
          Membership m = Membership.factorize(triangle);
          log.trace(String.format("triangle %s, binned unhip key %s.",
              triangle, m));
          ctx.write(m, new GenericGraphElement(triangle));
        }
      }
    }
  }
}
