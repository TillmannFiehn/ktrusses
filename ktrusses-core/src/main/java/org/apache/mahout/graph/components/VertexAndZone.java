package org.apache.mahout.graph.components;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.graph.model.Vertex;

import com.google.common.collect.ComparisonChain;

public class VertexAndZone implements WritableComparable<VertexAndZone> {

  private Vertex vertex, zone;

  /**
   * Construct an empty instance
   */
  public VertexAndZone() {
  }

  public VertexAndZone(long v, long z) {
    this(new Vertex(v), new Vertex(z));
  }
  
  public VertexAndZone(Vertex v, Vertex z) {
    this.vertex = v;
    this.zone = z;
  }
  

  public Vertex getVertex() {
    return vertex;
  }
  public Vertex getZone() {
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
  public int compareTo(VertexAndZone o) {
    return ComparisonChain.start()
    .compare(vertex, o.vertex)
    .compare(zone, o.zone).result();
  }

  @Override
  public String toString() {
    return "(" + vertex + "->" + zone + ")";
  }

}
