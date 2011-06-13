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

public class VertexWithDegree implements Comparable<VertexWithDegree> {

  private long d;
  private Vertex v;

  public VertexWithDegree(Vertex v, long d) {
    this.d = d;
    this.v = v;
  }

  @Override
  public int compareTo(VertexWithDegree o) {
    int c = new Long(d).compareTo(new Long(o.d));
    if (c == 0) {
      c = v.compareTo(o.v);
    }
    return c;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof VertexWithDegree) {
      int c = compareTo((VertexWithDegree) o);
      return c == 0;
    } else if (o instanceof Vertex) {
      return ((Vertex) o).equals(this);
    } else {
      return false;
    }
  }

  public Vertex getVertex() {
    return v;
  }

  public long getDegree() {
    return d;
  }
}
