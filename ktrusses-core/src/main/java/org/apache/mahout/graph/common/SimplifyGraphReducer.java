/**
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
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
import java.util.Map;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.graph.model.Membership;
import org.apache.mahout.graph.model.RepresentativeEdge;

/**
 *
 */
public class SimplifyGraphReducer extends
    Reducer<Membership, RepresentativeEdge, Membership, RepresentativeEdge> {

	@Override
	public void reduce(Membership key, Iterable<RepresentativeEdge> values, Context ctx) throws InterruptedException, IOException {
		
		Map<RepresentativeEdge, RepresentativeEdge> edges = new HashMap<RepresentativeEdge, RepresentativeEdge>();
		for(RepresentativeEdge edge : values) {
			RepresentativeEdge prev = edges.get(edge);
			if (prev != null) {
				//TODO implement aggregation
			}
			edges.put(edge, edge);
		}
		for(RepresentativeEdge edge : edges.values()) {
			ctx.write(key, edge);
		}
	}
	
}
