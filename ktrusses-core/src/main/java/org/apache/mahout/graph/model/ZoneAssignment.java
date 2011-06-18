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
 * This is a {@link Zone} and {@GenericGraphElement} that
 * has been assigned the very {@link Zone}.
 */
public class ZoneAssignment implements WritableComparable<ZoneAssignment> {

  /**
   * the zone of this instance
   */
  private Zone z;

  /**
   * the graph element that is assigned the zone
   */
  private GenericGraphElement g;

  /**
   * Create an instance of a zone assignment.
   * 
   * @param z
   *          The zone to be assigned
   * @param g
   *          The graph element that is assigned the zone
   */
  public ZoneAssignment(Zone z, GenericGraphElement g) {
    this.z = z;
    this.g = g;
  }

  /**
   * Creates an empty instance.
   */
  public ZoneAssignment() {
  }

  /**
   * This method returns true if the other instance is either a
   * {@link ZoneAssignment } with degree equal to this instance and vertex equal
   * to this instance's vertex or other instance is a {@link Vertex} and equals
   * this instance's vertex.
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof ZoneAssignment) {
      boolean is = z.equals(((ZoneAssignment) o).z);
      is = is && g.equals(((ZoneAssignment) o).g);
      return is;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    VertexWithDegree v = z.getVertex();
    return String.format("%s (%d)", v.getVertex(), v.getDegree());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    z = new Zone();
    z.readFields(in);
    g = new GenericGraphElement();
    g.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    z.write(out);
    g.write(out);
  }

  /**
   * Compare by zone first and than by value. Return 0 only if both are equal.
   */
  @Override
  public int compareTo(ZoneAssignment o) {
    int zone = z.compareTo(o.z);
    int generic = g.compareTo(o.g);
    if (zone == 0 && generic == 0) {
      return 0;
    } else {
      if (zone != 0) {
        return zone;
      } else {
        return generic;
      }
    }
  }

  /**
   * Get the zone of this assignment
   * 
   * @return The zone
   */
  public Zone getZone() {
    return z;
  }

  /**
   * Get the graph element that was assigned the zone
   * 
   * @return The graph element
   */
  public GenericGraphElement getValue() {
    return g;
  }

}
