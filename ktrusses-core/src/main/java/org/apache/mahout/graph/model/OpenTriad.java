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
import java.util.Vector;

import org.apache.hadoop.io.WritableComparable;

/**
 * Data type for open triads which are an intermediate result on triangle
 * enumeration.
 * 
 */
public class OpenTriad implements WritableComparable<OpenTriad> {

  /**
   * the apex of the instance
   */
  private Vertex apex;
  /**
   * the edges of the instance
   */
  protected Vector<RepresentativeEdge> edges;

  /**
   * Construct an open triad
   */
  public OpenTriad() {
    edges = new Vector<RepresentativeEdge>(2);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    apex.write(out);
    int c = edges.size();
    out.writeInt(c);
    for (RepresentativeEdge edge : edges) {
      edge.write(out);
    }

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    apex = new Vertex();
    apex.readFields(in);
    int c = in.readInt();
    edges = new Vector<RepresentativeEdge>(c);
    while (c-- > 0) {
      RepresentativeEdge edge = new RepresentativeEdge();
      edge.readFields(in);
      edges.add(edge);
    }

  }

  /**
   * Set the apex of the triad.
   * 
   * @param v
   *          The vertex the apex is to be set to
   */
  public void setApex(Vertex v) {
    apex = v;
  }

  /**
   * Add an edge to the edges
   * 
   * @param edge
   *          The edge to be added
   */
  public void addEdge(RepresentativeEdge edge) {
    edges.add(edge);
  }

  /**
   * Get the apex of the triad
   * 
   * @return the apex of this instance
   */
  public Vertex getApex() {
    return apex;
  }

  /**
   * Get the edges that were set to the instance.
   * 
   * @return a vector of edges or an empty vector if no edges were set
   */
  public Vector<RepresentativeEdge> getEdges() {
    return edges;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof OpenTriad) {
      OpenTriad t = (OpenTriad) o;
      // TODO check if this holds
      return apex.equals(t.apex) && edges.equals(t.edges);
    }
    return false;
  }

  @Override
  public int compareTo(OpenTriad o) {
    // TODO Auto-generated method stub
    return 0;
  }
}
