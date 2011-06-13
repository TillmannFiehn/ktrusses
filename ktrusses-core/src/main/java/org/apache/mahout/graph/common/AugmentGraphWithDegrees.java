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

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.Vertex;

public class AugmentGraphWithDegrees {

  public static class ScatterEdges extends
          Mapper<Object, RepresentativeEdge, Membership, RepresentativeEdge> {

    @Override
    public void map(Object key, RepresentativeEdge value, Context ctx)
            throws IOException, InterruptedException {

      ctx.write(new Membership().addMember(value.getVertex0()), value);
      ctx.write(new Membership().addMember(value.getVertex1()), value);

    }
  }

  public static class SumDegrees extends
          Reducer<Membership, RepresentativeEdge, Membership, RepresentativeEdge> {

    @Override
    public void reduce(Membership key, Iterable<RepresentativeEdge> values,
            Context ctx) throws IOException, InterruptedException {
      long degree = 0;
      Iterator<RepresentativeEdge> i = values.iterator();
      while (i.hasNext()) {
        i.next();
        degree++;
      }

      Vertex v = key.getMembers().iterator().next();

      Iterator<RepresentativeEdge> j = values.iterator();
      while (j.hasNext()) {
        RepresentativeEdge edge = j.next();
        edge.setDegree(v, degree);
        ctx.write(
                new Membership().addMember(edge.getVertex0()).addMember(
                edge.getVertex1()), edge);
      }

    }
  }

  public static class JoinDegrees extends
          Reducer<Membership, RepresentativeEdge, Membership, RepresentativeEdge> {

    @Override
    public void reduce(Membership key, Iterable<RepresentativeEdge> values,
            Context ctx) throws IOException, InterruptedException {
      long d0 = Integer.MIN_VALUE;
      long d1 = Integer.MIN_VALUE;
      for (RepresentativeEdge edge : values) {

        if (d0 == Integer.MIN_VALUE) {
          Vertex v = edge.getVertex0();
          d0 = edge.getDegree(v);
        }

        if (d1 == Integer.MIN_VALUE) {
          Vertex v = edge.getVertex1();
          d0 = edge.getDegree(v);
        }

      }

      RepresentativeEdge edge = values.iterator().next();

      Vertex v0 = edge.getVertex0();
      edge.setDegree(v0, d0);
      Vertex v1 = edge.getVertex1();
      edge.setDegree(v1, d1);

      ctx.write(key, edge);
    }
  }
}
