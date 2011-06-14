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

/**
 * Class representing a vertex in the graph to be analyzed.
 * 
 */
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

  /**
   * the id to identify this instance
   */
  private long id;

  /**
   * Constructs an empty vertex
   */
  public Vertex() {
  }

  /**
   * Construct a vertex with <code>id</code> set to parameter <code>id</code>
   * 
   * @param id
   *          The Vertex's id
   */
  public Vertex(long id) {
    this.id = id;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.id);
  }

  /**
   * Set the <code>id</code> to parameter <code>id</code>
   * 
   * @param id
   *          The <code>id</code> to apply to this instance
   */
  public void setId(long id) {
    this.id = id;
  }

  /**
   * Get the id of the vertex
   * 
   * @return The id of the vertex
   */
  public long getId() {
    return this.id;
  }

  /**
   * Compares this instance to another according to the <code>id</code>
   * attribute.
   */
  @Override
  public int compareTo(Vertex o) {
    return new Long(getId()).compareTo(new Long(o.getId()));
  }

  /**
   * Compares this instance to another according to the <code>id</code>
   * attribute. The other instance can be a {@link VertexWithDegree} in which
   * case the <code>id</code> is compared as well. Different degrees are
   * silently ignored.
   */
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

  /**
   * The hash code the <code>id</code> attribute
   */
  @Override
  public int hashCode() {
    return (int) id;
  }
}
