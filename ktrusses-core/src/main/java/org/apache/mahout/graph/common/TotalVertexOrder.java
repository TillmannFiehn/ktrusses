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

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.Vertex;
import org.apache.mahout.graph.model.VertexWithDegree;

public class TotalVertexOrder implements Comparator<VertexWithDegree> {

  private static final TotalVertexOrder order = new TotalVertexOrder();

  public static TotalVertexOrder instance() {
    return order;
  }

  @Override
  public int compare(VertexWithDegree v, VertexWithDegree w) {
    int c = 0;
    if (v == null && w == null) {
      c = 0;
    } else if (v == null && w != null) {
      c = Integer.MIN_VALUE;
    } else if (v != null && w == null) {
      c = Integer.MAX_VALUE;
    } else {
      c = v.compareTo(w);
    }
    return c;
  }

  public static Set<VertexWithDegree> getOrdered(RepresentativeEdge edge) {
    Set<VertexWithDegree> vertices = new TreeSet<VertexWithDegree>();
    Vertex v0 = edge.getVertex0();
    vertices.add(new VertexWithDegree(v0, edge.getDegree(v0)));
    Vertex v1 = edge.getVertex1();
    vertices.add(new VertexWithDegree(v1, edge.getDegree(v1)));
    return vertices;
  }
}
