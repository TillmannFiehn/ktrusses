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

package org.apache.mahout.graph.components;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.graph.model.Vertex;

import com.google.common.collect.ComparisonChain;

public class VertexOrRepresentative implements WritableComparable<VertexOrRepresentative> {

  private Vertex vertex, zone;

  /**
   * Construct an empty instance
   */
  public VertexOrRepresentative() {
  }

  public VertexOrRepresentative(long v, long z) {
    this(new Vertex(v), new Vertex(z));
  }
  
  public VertexOrRepresentative(Vertex v, Vertex z) {
    this.vertex = v;
    this.zone = z;
  }
  

  public Vertex getVertex() {
    return vertex;
  }
  public Vertex getRepresentative() {
    return zone;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    boolean hasVertex = in.readBoolean();
    if(hasVertex) {
      vertex = new Vertex();
      vertex.readFields(in);
    }
    boolean hasZone = in.readBoolean();
    if(hasZone) {
      zone = new Vertex();
      zone.readFields(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    boolean hasVertex = (vertex != null);
    out.writeBoolean(hasVertex);
    if(hasVertex) {
      vertex.write(out);
    }
    boolean hasZone = (zone != null);
    out.writeBoolean(hasZone);
    if(hasZone) {
      zone.write(out);
    }
  }

  @Override
  public int compareTo(VertexOrRepresentative o) {
    return ComparisonChain.start()
    .compare(vertex, o.vertex)
    .compare(zone, o.zone).result();
  }

  @Override
  public String toString() {
    return "(" + vertex + "->" + zone + ")";
  }

}
