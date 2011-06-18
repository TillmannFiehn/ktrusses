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

/**
 * This package brings some graph algorithms to
 * <em>Mahout</em>.
 * <ol>
 * 	<li>{@link org.apache.mahout.graph.common} for a tool chain to
 * 		prepare arbitrary graphs</li>
 * 	<li>{@link org.apache.mahout.graph.model} to get to know the
 * 		formats to use the results for further processing</li>
 * 	<li>{@link org.apache.mahout.graph.components.FindComponentsJob}
 * 		which finds the components of a graph.</li>
 * 	<li>{@link org.apache.mahout.graph.components.FindKTrussesJob}
 * 		which finds the <code>k</code>-trusses in a graph. A <code>k</code>-truss
 * 		is a nontrivial, single-component maximal subgraph, such that every
 * 		edge is contained in at least <code>k-2</code> triangles in the
 * 		subgraph. The algorithm was proposed in the IEEE paper <em>J.
 * 			Cohen 2009: "Graph Twiddling in a MapReduce World"</em>.</li>
 * </ol>
 */
package org.apache.mahout.graph;
