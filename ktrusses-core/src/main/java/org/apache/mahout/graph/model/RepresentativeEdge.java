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
 * A representative edge to be operated on in several graph algorithms.
 */
public class RepresentativeEdge extends Edge implements
    WritableComparable<RepresentativeEdge> {

  /**
   * Make a deep copy of the stated object.
   * 
   * @param orig
   *          The object to be copied
   * @return A deep copy
   */
  public static RepresentativeEdge duplicate(RepresentativeEdge orig) {
    RepresentativeEdge dup = new RepresentativeEdge();
    dup.d0 = orig.d0;
    dup.d1 = orig.d1;
    dup.v0 = Vertex.duplicate(orig.v0);
    dup.v1 = Vertex.duplicate(orig.v1);
    return dup;
  }

  /**
   * the vertices
   */
  protected Vertex v0, v1;
  /**
   * its degrees
   */
  private long d0 = Integer.MIN_VALUE, d1 = Integer.MIN_VALUE;

  /**
   * Construct an empty instance
   */
  public RepresentativeEdge() {
  }

  /**
   * Constructs a edge between the stated vertices.
   * 
   * @param v0
   *          The first vertex
   * @param v1
   *          The second vertex
   */
  public RepresentativeEdge(Vertex v0, Vertex v1) {
    this.v0 = v0;
    this.v1 = v1;
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

  /**
   * Returns true if the other instance is an edge containing the same vertices
   * in same order.
   */
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

  /**
   * Set the first vertex to <code>v</code>
   * 
   * @param v
   *          The vertex the first attribute it to be set to
   */
  public void setVertex0(Vertex v) {
    v0 = v;
  }

  /**
   * Set the second vertex to <code>v</code>
   * 
   * @param v
   *          The vertex the second attribute it to be set to
   */
  public void setVertex1(Vertex v) {
    v1 = v;
  }

  /**
   * Get the first vertex of this edge.
   * 
   * @return The first vertex
   */
  public Vertex getVertex0() {
    return v0;
  }

  /**
   * Get the second vertex of this edge.
   * 
   * @return The second vertex
   */
  public Vertex getVertex1() {
    return v1;
  }

  /**
   * The hash code is the product of the instance's vertices ids.
   */
  @Override
  public int hashCode() {
    return 1 * v0.hashCode() * v1.hashCode();
  }

  /**
   * Augment this edge with degree information for the vertex <code>v</code>
   * 
   * @param v
   *          The vertex to be augmented.
   * @param degree
   *          The augmentation value degree
   */
  public void setDegree(Vertex v, Long degree) {
    if (v0.equals(v)) {
      d0 = degree;
    } else if (v1.equals(v)) {
      d1 = degree;
    } else {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Get a degree for a vertex of this edge
   * 
   * @param v
   *          the vertex which is the augmented degree to be looked up for
   * @return the degree if it has been augmented before
   *         {@link Integer #MIN_VALUE} otherwise
   * @throws IllegalArgumentException
   *           if the vertex <code>v</code> does not belong to this edge
   */
  public long getDegree(Vertex v) throws IllegalArgumentException {
    if (v0.equals(v)) {
      return d0;
    } else if (v1.equals(v)) {
      return d1;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public String toString() {
    return String.format("(%d (%s)) <-> (%d (%s))", v0.getId(),
        (d0 >= 0 ? new Long(d0).toString() : "-"), v1.getId(),
        (d1 >= 0 ? new Long(d1).toString() : "-"));
  }

  /**
   * Compares two edges by {@link Membership}
   */
  @Override
  public int compareTo(RepresentativeEdge o) {
    Membership i = Membership.factorize(this);
    Membership it = Membership.factorize(o);
    return i.compareTo(it);
  }
}
