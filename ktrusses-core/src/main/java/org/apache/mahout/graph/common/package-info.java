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
