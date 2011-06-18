package org.apache.mahout.graph.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Wrapper class around {@link VertexWithDegree} for type safety.
 */
public class Zone implements WritableComparable<Zone> {

  /**
   * The only attribute, since this class is just for type safety
   */
  private VertexWithDegree v;

  /**
   * Construct an empty instance
   */
  public Zone() {
  }

  /**
   * Equivalent to <code>new Zone().setVertexWithDegree(v)</code>.
   * 
   * @param v
   *          The instance of {@link VertexWithDegree} to be wrapped
   */
  public Zone(VertexWithDegree v) {
    this();
    setVertex(v);
  }

  /**
   * Set the only attribute
   * 
   * @param v
   *          The instance of {@link VertexWithDegree} to be wrapped
   * @return this for convenience
   */
  public Zone setVertex(VertexWithDegree v) {
    this.v = v;
    return this;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    v = new VertexWithDegree();
    v.readFields(in);

  }

  @Override
  public void write(DataOutput out) throws IOException {
    v.write(out);
  }

  /**
   * Equivalent to {@link VertexWithDegree #compareTo(VertexWithDegree)}
   */
  @Override
  public int compareTo(Zone o) {
    return v.compareTo(o.v);
  }

  /**
   * Getter for the only attribute
   * 
   * @return The wrapped instance
   */
  public VertexWithDegree getVertex() {
    return v;
  }

}
