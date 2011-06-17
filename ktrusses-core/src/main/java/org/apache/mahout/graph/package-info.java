/**
 * This package brings some graph algorithms to
 * <em>Mahout</em>.
 * <ol>
 * 	<li><code>org.apache.mahout.graph.common</code> for a tool chain to
 * 		prepare arbitrary graphs</li>
 * 	<li><code>org.apache.mahout.graph.model</code> to get to know the
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
