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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.graph.model.UndirectedEdge;
import org.apache.mahout.graph.model.Vertex;
import org.apache.mahout.graph.triangles.EnumerateTrianglesJob;

/**
 * Finds components of a graph.
 * 
 */
public class FindComponentsJob extends AbstractJob {

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new EnumerateTrianglesJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    addInputOption();
    addOutputOption();

    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }

    Path tempDirPath = new Path(parsedArgs.get("--tempDir"));

    Path inputPath = getInputPath();
    Path outputPath = getOutputPath();

    // necessary as long as we don't have access to an undeprecated
    // MultipleInputs

    // scatter the edges each vertices and forward zones

    //
    return 0;
  }

  public static class PrepareInputMapper extends
      Mapper<Vertex, Vertex, JoinableVertex, VertexOrRepresentative> {

    @Override
    public void map(Vertex from, Vertex to, Context ctx) throws IOException,
        InterruptedException {
      // output the edge
      ctx.write(new JoinableVertex(from, false), new VertexOrRepresentative(to,
          null));
      // assign zone representatives to each node
      ctx.write(new JoinableVertex(from, true), new VertexOrRepresentative(
          null, from));
      ctx.write(new JoinableVertex(to, true), new VertexOrRepresentative(null,
          from));
    }
  }

  public static class PrepareInputReducer
      extends
      Reducer<JoinableVertex, VertexOrRepresentative, JoinableVertex, VertexOrRepresentative> {

    @Override
    public void reduce(JoinableVertex from,
        Iterable<VertexOrRepresentative> verticesOrRepresentatives, Context ctx)
        throws IOException, InterruptedException {
      TreeSet<Long> ids = new TreeSet<Long>();
      for (VertexOrRepresentative vertexOrRepresentative : verticesOrRepresentatives) {
        if (vertexOrRepresentative.getRepresentative() == null) {
          // output the edge
          ctx.write(new JoinableVertex(from.getVertex(), false),
              new VertexOrRepresentative(vertexOrRepresentative.getVertex()
                  .clone(), null));
        } else {
          // output just one representative
          ids.add(vertexOrRepresentative.getRepresentative().getId());
        }
      }

      ctx.write(new JoinableVertex(from.getVertex(), true),
          new VertexOrRepresentative(null, new Vertex(ids.iterator().next())));

    }
  }

  /**
   * Scatters zone assignments and edges. This {@link Mapper} has two inputs:
   * 
   * <ul>
   * <li>edge file</li>
   * <li>zone file</li>
   * </ul>
   * 
   * Forward the zone assignments.<br />
   * Forward the edges to each of the vertices.
   */
  public static class ScatterEdgesAndForwardZoneAssignmentsMapper extends
      Mapper<Vertex, VertexOrRepresentative, JoinableVertex, VertexOrRepresentative> {

    @Override
    public void map(Vertex first,
        VertexOrRepresentative secondOrRepresentative, Context ctx)
        throws IOException, InterruptedException {
      if (secondOrRepresentative.getRepresentative() == null) {
        Vertex second = secondOrRepresentative.getVertex();
        // write the edge to each vertex
        ctx.write(new JoinableVertex(first, false), new VertexOrRepresentative(second, null));
        ctx.write(new JoinableVertex(second, false), new VertexOrRepresentative(first, null));
      } else {
        // forward the zone
        ctx.write(new JoinableVertex(first, true), secondOrRepresentative);
      }
    }
  }

  /**
   * Joins zones and edges. Input is a {@link ZoneAssignment} and a set of edges
   * the incident to the vertex that is assigned with the assignment.
   * <p>
   * This class joins {@link Zone} to all the edges of the input, outputting the
   * {@link Zone} as value, binned under the membership set of the edge.
   */
  public static class AssignOneZoneToEdgesReducer
      extends
      Reducer<JoinableVertex, VertexOrRepresentative, UndirectedEdge, VertexOrRepresentative> {
    @Override
    public void reduce(JoinableVertex first,
        Iterable<VertexOrRepresentative> verticesAndZone, Context ctx)
        throws IOException, InterruptedException {
      // FIXME implement the lineage flag to avoid memory consumption
      Iterator<VertexOrRepresentative> iterator = verticesAndZone.iterator();
      VertexOrRepresentative assignment = iterator.next();
      while (iterator.hasNext()) {
        VertexOrRepresentative secondVertex = iterator.next();
        Vertex second = secondVertex.getVertex();
        ctx.write(new UndirectedEdge(first.getVertex(), second), assignment);
      }
    }
  }

  /**
   * Find the minimum zone for each edge.
   * <p>
   * For each other zone, output an interzone edge, which is a record with key
   * other zone and value minimum zone that is to be assigned.
   */
  public static class FindInterzoneEdgesReducer
      extends
      Reducer<UndirectedEdge, VertexOrRepresentative, JoinableVertex, VertexOrRepresentative> {
    @Override
    public void reduce(UndirectedEdge edge,
        Iterable<VertexOrRepresentative> assignments, Context ctx)
        throws IOException, InterruptedException {
      Set<Long> ids = new TreeSet<Long>();
      for (VertexOrRepresentative ass : assignments) {
        ids.add(ass.getRepresentative().getId());
      }
      Iterator<Long> i = ids.iterator();
      Vertex minZone = new Vertex(i.next());
      for (Long other : ids) {
        ctx.write(new JoinableVertex(other, true), new VertexOrRepresentative(
            null, minZone));
      }
    }
  }

  public static class BinZoneAssignmentsAndInterzones
      extends
      Mapper<JoinableVertex, VertexOrRepresentative, JoinableVertex, VertexOrRepresentative> {

    @Override
    public void map(JoinableVertex vertex,
        VertexOrRepresentative vertexOrRepresentative, Context ctx)
        throws IOException, InterruptedException {
      
      if(vertex.isMarked()) {
        // forward the interzone edge
        ctx.write(vertex, vertexOrRepresentative);
      } else {
        if(vertexOrRepresentative.getRepresentative() == null) {
          //edge -> forward
          // TODO create extra edges file to avoid overhead
          ctx.write(vertex, vertexOrRepresentative);
        } else {
          // assignment -> bin the vertex assigned under the zone
          ctx.write(new JoinableVertex(vertexOrRepresentative.getRepresentative(), false), new VertexOrRepresentative(vertex.getVertex(), null));
        }
      }

    }

  }

}
