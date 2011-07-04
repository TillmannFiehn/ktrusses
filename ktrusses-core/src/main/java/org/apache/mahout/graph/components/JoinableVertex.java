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
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.mahout.graph.model.Vertex;

public class JoinableVertex implements WritableComparable<JoinableVertex> {

  private Vertex vertex;
  private boolean marked;

  static {
    WritableComparator.define(JoinableVertex.class, new SecondarySortComparator());
  }

  public JoinableVertex() {}

  public JoinableVertex(Vertex vertex, boolean marked) {
    this.vertex = vertex;
    this.marked = marked;
  }


  public JoinableVertex(long id, boolean marked) {
    this(new Vertex(id), marked);
  }

  public Vertex getVertex() {
    return vertex;
  }
  
  public boolean isMarked() {
    return marked;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    vertex.write(out);
    out.writeBoolean(marked);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    vertex = new Vertex();
    vertex.readFields(in);
    marked = in.readBoolean();
  }

  @Override
  public int compareTo(JoinableVertex other) {
    return vertex.compareTo(other.vertex);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof JoinableVertex) {
      return vertex.equals(((JoinableVertex) o).vertex);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return vertex.hashCode();
  }

  @Override
  public String toString() {
    return "(" + vertex + "," +marked + ")";
  }

  @SuppressWarnings("serial")
  public static class SecondarySortComparator extends WritableComparator implements Serializable {

    protected SecondarySortComparator() {
      super(JoinableVertex.class, true);
    }

    @Override
    public int compare(@SuppressWarnings("rawtypes") WritableComparable a, @SuppressWarnings("rawtypes") WritableComparable b) {
      JoinableVertex first = (JoinableVertex) a;
      JoinableVertex second = (JoinableVertex) b;

      int result = first.vertex.compareTo(second.vertex);
      if (result == 0) {
        if (first.marked && !second.marked) {
          return -1;
        } else if (!first.marked && second.marked) {
          return 1;
        }
      }
      return result;
    }

  }

  @SuppressWarnings("serial")
  public static class GroupingComparator extends WritableComparator implements Serializable {
    protected GroupingComparator() {
      super(JoinableVertex.class, true);
    }
  }
}
