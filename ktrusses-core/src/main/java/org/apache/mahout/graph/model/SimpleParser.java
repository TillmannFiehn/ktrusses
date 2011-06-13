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

import java.util.TreeSet;
import java.util.Vector;

import org.apache.hadoop.io.Text;

/**
 * Simple parser for graph input files. It takes edges as comma separated list
 * of vertices. For example a simple input file could look like this:
 * 
 * <pre>
 *    1,2
 *    2,3
 *    3,1
 * </pre>
 * 
 */
public class SimpleParser implements Parser {

  /**
   * Parses a simple graph input file that contains a comma separated list of
   * numbers representing the vertices. It silently ignores all lines that do
   * not match the patter <tt>A,B</tt>
   */
  @Override
  public Vector<Vertex> parse(Text description) {
    try {
      String[] splits = description.toString().split(",");
      TreeSet<Vertex> set = new TreeSet<Vertex>();
      for (String s : splits) {
        final long d = Long.parseLong(s);
        Vertex v = new Vertex(d);
        set.add(v);
      }
      Vector<Vertex> vec = new Vector<Vertex>(set);
      return vec;
    } catch (Exception e) {
      return null;
    }
  }
}
