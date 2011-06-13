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

import org.apache.hadoop.io.WritableComparable;

public class Vertex implements WritableComparable<Vertex> {

  public static Vertex read(DataInput in) throws IOException {
    Vertex v = new Vertex();
    v.readFields(in);
    return v;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readLong();
  }
  private long id;

  public Vertex() {
  }

  public Vertex(long id) {
    this.id = id;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.id);
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getId() {
    return this.id;
  }

  @Override
  public int compareTo(Vertex o) {
    return new Long(getId()).compareTo(new Long(o.getId()));
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Vertex) {
      return ((Vertex) o).id == id;
    } else if (o instanceof VertexWithDegree) {
      return ((VertexWithDegree) o).getVertex().equals(this);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return (int) id;
  }
}
