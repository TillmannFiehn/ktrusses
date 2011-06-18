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
 * This package contains some common tasks for graph algorithms. It
 * proposes a standard tool chain to process a graph:
 * <ol>
 * 	<li>Simplify the graph with {@link
 * 		org.apache.mahout.graph.common.SimplifyGraphJob}. Parse a text file
 * 		to a small representation of edges (we drop the vertices without
 * 		edges) without loops and duplicate edges. After this step the graph
 * 		is interpreted as an undirected graph.</li>
 * 	<li>Augment the graph with vertex degrees using {@link
 * 		org.apache.mahout.graph.common.AugmentGraphWithDegreesJob}. This
 * 		ensures some stability and can be achieved with two MapReduce
 * 		pipelines.</li>
 * 	<li>Enumerate the triangles of the augmented simplified graph
 * 		with {@link org.apache.mahout.graph.common.EnumerateTrianglesJob}
 * 		which is a good starting point for further processing.</li>
 * </ol>
 */
package org.apache.mahout.graph.common;
