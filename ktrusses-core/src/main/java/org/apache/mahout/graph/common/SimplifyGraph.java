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

package org.apache.mahout.graph.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.graph.model.GenericGraphElement;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.Parser;
import org.apache.mahout.graph.model.RepresentativeEdge;
import org.apache.mahout.graph.model.SimpleParser;
import org.apache.mahout.graph.model.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container for the {@link SimplifyGraphJob } mapper and reducer classes.
 * 
 */
public class SimplifyGraph {

  private static Logger log = LoggerFactory.getLogger(SimplifyGraph.class);
  
  /**
   * Bins edges by an ordered membership set. Scatters edges with at least two
   * vertices in the membership set.
   * 
   */
  public static class SimplifyGraphMapper extends
      Mapper<Object, Text, Membership, GenericGraphElement> {

    Parser parser;

    @Override
    public void setup(Context ctx) {
      Configuration conf = ctx.getConfiguration();
      String classname = conf.get(Parser.class.getCanonicalName());
      if( classname!=null ) try {
        @SuppressWarnings("unchecked")
        Class<Parser> parserclass = (Class<Parser>) Class.forName(classname);
        parser = (Parser) parserclass.newInstance();
      } catch (ClassNotFoundException e) {
        log.error(e.getMessage());
        log.warn(e.getMessage(), e);
      } catch (InstantiationException e) {
        log.error(e.getMessage());
        log.warn(e.getMessage(), e);
      } catch (IllegalAccessException e) {
        log.error(e.getMessage());
        log.warn(e.getMessage(), e);
      }
      if (parser == null) {
        parser = new SimpleParser();
      }

    }

    @Override
    public void map(Object key, Text description, Context ctx)
        throws IOException, InterruptedException {

      Vector<Vertex> members = parser.parse(description);
      if (members != null && members.size() > 1) {
        Iterator<Vertex> i = members.iterator();
        Vertex v0 = i.next();
        Vertex v1 = i.next();
        RepresentativeEdge edge = new RepresentativeEdge(v0, v1);
        Membership mem = Membership.factorize(edge);
        log.trace(String.format(
            "representative no-loop edge %s, binned under %s.",
            edge, mem));
        ctx.write(mem, new GenericGraphElement(edge));
      }

    }
  }

  /**
   * Removes duplicate edges.
   */
  public static class SimplifyGraphReducer extends
      Reducer<Membership, GenericGraphElement, Membership, GenericGraphElement> {

    @Override
    public void reduce(Membership key, Iterable<GenericGraphElement> generics,
        Context ctx) throws InterruptedException, IOException {

      Map<RepresentativeEdge, RepresentativeEdge> edges = new HashMap<RepresentativeEdge, RepresentativeEdge>();
      for (GenericGraphElement generic : generics) {
        RepresentativeEdge edge = (RepresentativeEdge) generic.getValue();
        RepresentativeEdge prev = edges.get(edge);
        if (prev != null) {
          // TODO implement aggregation
        }
        edges.put(edge, edge);
      }
      for (RepresentativeEdge edge : edges.values()) {
        log.trace(String.format(
            "representative no-loop unique edge %s, binned under %s.",
            edge, key));
        ctx.write(key, new GenericGraphElement(edge));
      }
    }
  }
}
