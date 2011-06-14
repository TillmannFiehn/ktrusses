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

/**
 * Container for a vertex augmented with a degree. The degree is interpreted as
 * the count of edges that start or end at the vertex.
 * 
 */
public class VertexWithDegree implements Comparable<VertexWithDegree> {

  /**
   * the degree of this instance
   */
  private long d;
  /**
   * the vertex of this instance
   */
  private Vertex v;

  /**
   * Create an instance of an augmented vertex.
   * 
   * @param v
   *          The vertex to be augmented
   * @param d
   *          The degree to be the augmentation
   */
  public VertexWithDegree(Vertex v, long d) {
    this.d = d;
    this.v = v;
  }

  /**
   * Compares the degree of this instance to the other and returns a negative
   * number if this instance's degree is lower, 0 if the degrees equal and a
   * positive number if this instance's degree is bigger than the other's.
   */
  @Override
  public int compareTo(VertexWithDegree o) {
    int c = new Long(d).compareTo(new Long(o.d));
    return c;
  }

  /**
   * This method returns true if the other instance is either a
   * {@link VertexWithDegree } with degree equal to this instance and vertex
   * equal to this instance's vertex or other instance is a {@link Vertex} and
   * equals this instance's vertex.
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof VertexWithDegree) {
      int c = compareTo((VertexWithDegree) o);
      return c == 0 && v.equals(((VertexWithDegree) o).v);
    } else if (o instanceof Vertex) {
      return ((Vertex) o).equals(this);
    } else {
      return false;
    }
  }

  /**
   * Getter for the <code>v</code> attribute.
   * 
   * @return This instance's vertex
   */
  public Vertex getVertex() {
    return v;
  }

  /**
   * Getter for the <code>d</code> attribute
   * 
   * @return The degree of this instance
   */
  public long getDegree() {
    return d;
  }
}
