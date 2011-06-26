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
  public static class ScatterEdgesToEachNodeAndForwardZoneAssignmentsMapper
      extends
      Mapper<Vertex, VertexAndZone, Vertex, VertexAndZone> {
    
    private boolean emitZones;
        
    @Override
    public void map(Vertex first, VertexAndZone secondOrZone, Context ctx)
        throws IOException, InterruptedException {
      if(secondOrZone.getZone() == null) {
        Vertex second = secondOrZone.getVertex();
        // write the edge to each vertex
        ctx.write(first, new VertexAndZone(second, null));
        ctx.write(second, new VertexAndZone(first, null));
      } else {
        // forward the zone
        ctx.write(first, secondOrZone);
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
  public static class AssignOneZoneToEdges extends
      Reducer<Vertex, VertexAndZone, UndirectedEdge, VertexAndZone> {
    @Override
    public void reduce(Vertex first, Iterable<VertexAndZone> verticesAndZone,
        Context ctx) throws IOException, InterruptedException {
      // FIXME implement the lineage flag to avoid memory consumption
      Iterator<VertexAndZone> iterator = verticesAndZone.iterator();
      VertexAndZone assignment = iterator.next();
      while (iterator.hasNext()) {
        VertexAndZone secondVertex = iterator.next();
        Vertex second = secondVertex.getVertex();
        ctx.write(new UndirectedEdge(first, second), assignment);
      }
    }
  }

  /**
   * Find the minimum zone for each edge.
   * <p>
   * For each other zone, output an interzone edge, which is a record with key
   * other zone and value minimum zone that is to be assigned.
   */
  public static class FindInterzoneEdges extends
      Reducer<UndirectedEdge, VertexAndZone, Vertex, VertexAndZone> {
    @Override
    public void reduce(UndirectedEdge edge, Iterable<VertexAndZone> assignments,
        Context ctx) throws IOException, InterruptedException {
      Set<Long> ids = new TreeSet<Long>();
      for (VertexAndZone ass : assignments) {
        ids.add(ass.getZone().getId());
      }
      Iterator<Long> i = ids.iterator();
      Vertex minZone = new Vertex(i.next());
      for (Long other : ids) {
        ctx.write(new Vertex(other), new VertexAndZone(null, minZone));
      }
    }
  }


}
