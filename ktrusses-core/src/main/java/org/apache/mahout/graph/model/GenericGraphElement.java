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
 * Utility class to merge different model classes into one SequenceFile or
 * multiple SequenceFiles to one Mapper or Reducer
 * 
 */
public class GenericGraphElement implements
    WritableComparable<GenericGraphElement> {

  /**
   * Constants to distinguish between different types that can be contained by
   * {@link GenericGraphElement }
   */
  public static enum Type {
    /**
     * {@link RepresentativeEdge}
     */
    RepresentativeEdge,
    /**
     * {@link Vertex}
     */
    Vertex,
    /**
     * {@link OpenTriad}
     */
    OpenTriad,
    /**
     * {@link Triangle}
     */
    Triangle,
  }

  /**
   * Save the type as a {@link Type } field here. Represented as an integer
   * ordinal in the files
   */
  private Type type;

  /**
   * The true value
   */
  @SuppressWarnings("rawtypes")
  private WritableComparable value;

  /**
   * Empty constructor for the deserialization methods
   */
  public GenericGraphElement() {

  }

  /**
   * Construct this container from a real object
   * 
   * @param it
   *          The object to be saved in a generic format
   */
  @SuppressWarnings("rawtypes")
  public GenericGraphElement(WritableComparable it) {
    value = it;
    try {
      type = Type.valueOf(it.getClass().getSimpleName());
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Get the real object
   * 
   * @return The real value object which can be cast
   */
  @SuppressWarnings("rawtypes")
  public WritableComparable getValue() {
    return value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(type.ordinal());
    value.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int ordinal = in.readInt();
    type = Type.values()[ordinal];
    switch (type) {
    case RepresentativeEdge:
      value = new RepresentativeEdge();
      break;
    case Vertex:
      value = new Vertex();
      break;
    case OpenTriad:
      value = new OpenTriad();
      break;
    case Triangle:
      value = new Triangle();
      break;
    default:
      throw new IllegalArgumentException();
    }
    value.readFields(in);

  }

  /**
   * Compares the values to each other if the type is equal
   * 
   * @throws IllegalArgumentException
   *           If the type differs
   */
  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(GenericGraphElement o) {
    if (type.equals(o.type)) {
      return value.compareTo(o.value);
    } else {
      throw new IllegalArgumentException();
    }
  };
  
  @Override
  public String toString() {
    return value.toString();
  }

  @Override
  public boolean equals(Object o) {
    if(o instanceof GenericGraphElement) {
      GenericGraphElement g = (GenericGraphElement) o;
      return type.equals(g.type) && value.equals(g.value);
    } else {
      return false;
    }
  }
}
