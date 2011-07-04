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

  public static class PrepareAssignmentsFileMapper extends
      Mapper<Vertex, Vertex, Vertex, Vertex> {

    @Override
    public void map(Vertex from, Vertex to, Context ctx) throws IOException,
        InterruptedException {
      // assign zone representatives to each node
      ctx.write(from, from);
      ctx.write(to, from);
    }
  }

  public static class PrepareAssignmentsFileReducer extends
      Reducer<Vertex, Vertex, Vertex, FlaggedVertex> {

    @Override
    public void reduce(Vertex from, Iterable<Vertex> representatives,
        Context ctx) throws IOException, InterruptedException {
      TreeSet<Long> ids = new TreeSet<Long>();
      for (Vertex representative : representatives) {
        // output just one representative
        ids.add(representative.getId());
      }
      ctx.write(from, FlaggedVertex.createZoneAssignment(ids.iterator().next()));
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
      Mapper<Vertex, FlaggedVertex, JoinableVertex, FlaggedVertex> {

    @Override
    public void map(Vertex first, FlaggedVertex secondOrRepresentative,
        Context ctx) throws IOException, InterruptedException {
      switch (secondOrRepresentative.getType()) {
      case UndirectedEdge:
        Vertex second = secondOrRepresentative.getVertex();
        // write the edge to each vertex
        ctx.write(new JoinableVertex(first, false),
            FlaggedVertex.createUndirectedEdge(second));
        ctx.write(new JoinableVertex(second, false),
            FlaggedVertex.createUndirectedEdge(first));
        break;
      case ZoneAssignment:
        // forward the assignment
        ctx.write(new JoinableVertex(first, true), secondOrRepresentative);
        break;
      default:
        throw new IllegalArgumentException();

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
  public static class AssignOneZoneToEdgesReducer extends
      Reducer<JoinableVertex, FlaggedVertex, UndirectedEdge, FlaggedVertex> {
    @Override
    public void reduce(JoinableVertex first,
        Iterable<FlaggedVertex> verticesAndZone, Context ctx)
        throws IOException, InterruptedException {
      // FIXME implement the lineage flag to avoid memory consumption
      Iterator<FlaggedVertex> iterator = verticesAndZone.iterator();
      FlaggedVertex assignment = iterator.next();
      while (iterator.hasNext()) {
        Vertex second = iterator.next().getVertex();
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
  public static class FindInterzoneEdgesReducer extends
      Reducer<UndirectedEdge, FlaggedVertex, Vertex, FlaggedVertex> {
    @Override
    public void reduce(UndirectedEdge edge,
        Iterable<FlaggedVertex> assignments, Context ctx) throws IOException,
        InterruptedException {
      Set<Long> ids = new TreeSet<Long>();
      for (FlaggedVertex ass : assignments) {
        ids.add(ass.getVertex().getId());
      }
      Iterator<Long> i = ids.iterator();
      long minZone = i.next();
      for (Long other : ids) {
        ctx.write(new Vertex(other), FlaggedVertex.createInterzoneEdge(minZone));
      }
    }
  }

  public static class BinZoneAssignmentsAndInterzoneEdgesMapper extends
      Mapper<Vertex, FlaggedVertex, JoinableVertex, FlaggedVertex> {

    @Override
    public void map(Vertex vertex, FlaggedVertex vertexOrRepresentative,
        Context ctx) throws IOException, InterruptedException {
      switch (vertexOrRepresentative.getType()) {
      case InterzoneEdge:
        // forward the interzone edge
        ctx.write(new JoinableVertex(vertex, true), vertexOrRepresentative);
        break;
      case ZoneAssignment:
        // assignment -> bin the vertex assigned under the zone
        ctx.write(
            new JoinableVertex(vertexOrRepresentative.getVertex(), false),
            FlaggedVertex.createZoneEntry(vertex));
        break;
      }
    }
  }

  public static class AssignNewZonesToVerticesReducer extends
      Reducer<JoinableVertex, FlaggedVertex, Vertex, FlaggedVertex> {
    @Override
    public void reduce(JoinableVertex oldRepresentative,
        Iterable<FlaggedVertex> betterRepresentativesAndVertices, Context ctx)
        throws IOException, InterruptedException {
      Set<Long> ids = new TreeSet<Long>();
      for (FlaggedVertex vertexOrRepresentative : betterRepresentativesAndVertices) {
        switch (vertexOrRepresentative.getType()) {
        case InterzoneEdge:
          // assignment -> put the improved representative to set
          ids.add(vertexOrRepresentative.getVertex().getId());
          break;
        case ZoneEntry:
          // entry -> assign the best of the better representatives
          ctx.write(vertexOrRepresentative.getVertex(),
              FlaggedVertex.createZoneAssignment(ids.iterator().next()));
          break;
        default:
          throw new IllegalArgumentException();
        }
      }
    }
  }

}
