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

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.graph.model.GenericGraphElement;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.Triangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container for the {@link FindKTrussesJob} {@link Mapper} and {@link Reducer} classes.
 *
 */
public class FindKTrusses {

  /**
   * Constant to access the configuration for parameter <code>k</code>.
   */
  public final static String K = "K";

  private static Logger log = LoggerFactory.getLogger(FindKTrusses.class);

  /**
   * Reading a triangle file, emit a record for each edge involved in each
   * triangle.
   */
  public static class SplitTrianglesToEdges extends
      Mapper<Object, GenericGraphElement, Membership, GenericGraphElement> {

    @Override
    public void map(Object key, GenericGraphElement generic, Context ctx)
        throws IOException, InterruptedException {
      Triangle triangle = (Triangle) generic.getValue();
      for (RepresentativeEdge edge : triangle.getEdges()) {
        ctx.write(Membership.factorize(edge), new GenericGraphElement(edge));
      }
    }
  }

  /**
   * Keeps only the edges with sufficient support. 
   */
  public static class DropUnsupportedEdges extends
      Reducer<Membership, GenericGraphElement, Membership, GenericGraphElement> {

    /**
     * The parameter <code>k</code> of the algorithm.
     */
    private int k;

    @Override
    public void setup(Context ctx) {
      k = Integer.parseInt(ctx.getConfiguration().get(K));

    }

    @Override
    public void reduce(Membership key, Iterable<GenericGraphElement> generics,
        Context ctx) throws IOException, InterruptedException {
      int issupported = 0;
      RepresentativeEdge edge = null;
      for (GenericGraphElement generic : generics) {
        if (edge == null)
          edge = RepresentativeEdge.duplicate((RepresentativeEdge) generic
              .getValue());
        if (++issupported >= k - 2) {
          break;
        }
      }
      if (issupported >= k - 2) {
        log.trace(String
            .format("Writing edge %s with sufficent support.", edge));
        ctx.write(Membership.factorize(edge), new GenericGraphElement(edge));
      } else {
        log.trace(String.format("Dropping edge %s without sufficent support.",
            edge));
        // FIXME notify system that edges were dropped
      }

    }
  }
}
