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

package org.apache.mahout.graph.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.WritableComparable;

/**
 * Model class for triangles. The triangle is represented through the order set
 * of its three edges. There is no restriction on the number of
 * {@link RepresentativeEdge}s in this class but the methods
 * {@link #equals(Object)} and {@link #compareTo(Triangle)} will both throw
 * {@link IllegalArgumentException} if not both instances contain exactly three
 * edges.
 */
public class Triangle implements WritableComparable<Triangle> {

  /**
   * the three edges of the triangle
   */
  private Set<RepresentativeEdge> edges;

  public Triangle() {
    edges = new TreeSet<RepresentativeEdge>();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int c = edges.size();
    out.writeInt(c);
    for (RepresentativeEdge edge : edges) {
      edge.write(out);
    }

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int c = in.readInt();
    edges = new TreeSet<RepresentativeEdge>();
    while (c-- > 0) {
      RepresentativeEdge edge = new RepresentativeEdge();
      edge.readFields(in);
      edges.add(edge);
    }
  }

  /**
   * Add an edge to this instance
   * 
   * @param edge
   *          The edge to be added
   * @return this for convenient consecutive calls
   */
  public Triangle addEdge(RepresentativeEdge edge) {
    edges.add(edge);
    return this;
  }

  /**
   * Get the edges of this instance
   * 
   * @return The edges
   */
  public Set<RepresentativeEdge> getEdges() {
    return edges;
  }

  /**
   * Compares this instance to another as a sequence of comparisons of the
   * edges.
   */
  @Override
  public int compareTo(Triangle o) {
    int c = 0;
    if (!(edges.size() == 3) && !(o.edges.size() == 3)) {
      throw new IllegalArgumentException();
    }
    Iterator<RepresentativeEdge> i = edges.iterator();
    Iterator<RepresentativeEdge> it = o.edges.iterator();
    while (i.hasNext()) {
      c = i.next().compareTo(it.next());
      if (c == 0) {
        continue;
      } else {
        break;
      }
    }
    return c;
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof Triangle && compareTo((Triangle) o) == 0);
  }

  /**
   * The hash code is the product of the triangles's edge ids.
   */
  @Override
  public int hashCode() {
    int hash = 1;
    Iterator<RepresentativeEdge> i = edges.iterator();
    while (i.hasNext()) {
      hash *= i.next().hashCode();
    }
    return hash;
  }
}
