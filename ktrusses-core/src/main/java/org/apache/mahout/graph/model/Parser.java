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

import java.util.Vector;

import org.apache.hadoop.io.Text;

/**
 * Interface to be implemented in order to parse the graph input file of any job
 * to vertices and edges.
 * 
 */
public interface Parser {

  /**
   * This method is called on each line of the input file.
   * 
   * @param description
   *          The hole line
   * @return A list of vertices that describe the edge or <tt>null</tt> if no
   *         edge was described a that specific line
   */
  public Vector<Vertex> parse(Text description);

}
