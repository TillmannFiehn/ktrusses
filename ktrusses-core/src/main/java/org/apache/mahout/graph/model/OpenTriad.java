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

package org.apache.mahout.graph.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.WritableComparable;

public class OpenTriad implements WritableComparable<OpenTriad> {

  private Vertex apex;
  protected Vector<RepresentativeEdge> edges;

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

  public void setApex(Vertex v) {
    apex = v;
  }

  public void addEdge(RepresentativeEdge edge) {
    edges.add(edge);
  }

  public Vertex getApex() {
    return apex;
  }

  public Vector<RepresentativeEdge> getEdges() {
    return edges;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof OpenTriad) {
      OpenTriad t = (OpenTriad) o;
      //FIXME implement this
      return apex.equals(t.apex);
    }
    return false;
  }

  @Override
  public int compareTo(OpenTriad o) {
    // TODO Auto-generated method stub
    return 0;
  }
}
