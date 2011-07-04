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

import org.apache.hadoop.io.Writable;
import org.apache.mahout.graph.model.Vertex;

public class FlaggedVertex implements Writable {
  
  public static enum PayloadType { ZoneAssignment, UndirectedEdge, InterzoneEdge, ZoneEntry, };

  public static FlaggedVertex createZoneAssignment(Vertex payload) {
    return new FlaggedVertex(payload, PayloadType.ZoneAssignment);
  }

  public static FlaggedVertex createUndirectedEdge(Vertex payload) {
    return new FlaggedVertex(payload, PayloadType.UndirectedEdge);
  }

  public static FlaggedVertex createInterzoneEdge(Vertex payload) {
    return new FlaggedVertex(payload, PayloadType.InterzoneEdge);
  }

  public static FlaggedVertex createZoneEntry(Vertex payload) {
    return new FlaggedVertex(payload, PayloadType.ZoneEntry);
  }

  public static FlaggedVertex createZoneAssignment(long id) {
    return new FlaggedVertex(new Vertex(id), PayloadType.ZoneAssignment);
  }

  public static FlaggedVertex createUndirectedEdge(long id) {
    return new FlaggedVertex(new Vertex(id), PayloadType.UndirectedEdge);
  }

  public static FlaggedVertex createInterzoneEdge(long id) {
    return new FlaggedVertex(new Vertex(id), PayloadType.InterzoneEdge);
  }

  public static FlaggedVertex createZoneEntry(long id) {
    return new FlaggedVertex(new Vertex(id), PayloadType.ZoneEntry);
  }

  private Vertex payload;
  private PayloadType type;

  /**
   * Construct an empty instance
   */
  public FlaggedVertex() {
  }

  private FlaggedVertex(Vertex payload, PayloadType type) {
    this.payload = payload;
    this.type = type;
  }
  

  public Vertex getVertex() {
    return payload;
  }
  
  public PayloadType getType() {
    return type;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    type = PayloadType.values()[in.readInt()];
    payload = new Vertex();
    payload.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(type.ordinal());
    payload.write(out);
  }
  
  public boolean isUndirectedEdge() {
    return type.equals(PayloadType.UndirectedEdge);
  }

  public boolean isZoneAssignment() {
    return type.equals(PayloadType.ZoneAssignment);
  }

  public boolean isInterzoneEdge() {
    return type.equals(PayloadType.InterzoneEdge);
  }

  public boolean isZoneEntry() {
    return type.equals(PayloadType.ZoneEntry);
  }

  @Override
  public String toString() {
    return "(" + payload + " as " + type + ")";
  }

}
