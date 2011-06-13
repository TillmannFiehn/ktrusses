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

/**
 *
 */
public class RepresentativeEdge extends Edge {

  protected Vertex v0, v1;
  private long d0 = Integer.MIN_VALUE, d1 = Integer.MIN_VALUE;

  public RepresentativeEdge() {
  }

  public RepresentativeEdge(Vertex node0, Vertex node1) {
    this.v0 = node0;
    this.v1 = node1;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    v0.write(out);
    out.writeLong(d0);
    v1.write(out);
    out.writeLong(d1);
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    v0 = new Vertex();
    v0.readFields(in);
    d0 = in.readLong();

    v1 = new Vertex();
    v1.readFields(in);
    d1 = in.readLong();

  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RepresentativeEdge) {
      RepresentativeEdge e = (RepresentativeEdge) o;
      if (v0.equals(e.v0) && v1.equals(e.v1)) {
        return true;
      }
    }
    return false;
  }

  public void setVertex0(Vertex v) {
    v0 = v;
  }

  public void setVertex1(Vertex v) {
    v1 = v;
  }

  public Vertex getVertex0() {
    return v0;
  }

  public Vertex getVertex1() {
    return v1;
  }

  @Override
  public int hashCode() {
    return 1 * v0.hashCode() * v1.hashCode();
  }

  public void setDegree(Vertex v, Long degree) {
    if (v0.equals(v)) {
      d0 = degree;
    } else if (v1.equals(v)) {
      d1 = degree;
    } else {
      throw new IllegalArgumentException();
    }
  }

  public long getDegree(Vertex v) {
    if (v0.equals(v)) {
      return d0;
    } else if (v1.equals(v)) {
      return d1;
    } else {
      throw new IllegalArgumentException();
    }
  }
}
