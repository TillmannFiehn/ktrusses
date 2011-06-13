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

package org.apache.mahout.graph.model;

import java.util.TreeSet;
import java.util.Vector;

import org.apache.hadoop.io.Text;

public class SimpleParser implements Parser {

	@Override
	public Vector<Vertex> parse(Text description) {
		String[] splits = description.toString().split(",");
		TreeSet<Vertex> set = new TreeSet<Vertex>();
		for(String s : splits) {
			final long d = Long.parseLong(s);
			Vertex v = new Vertex(d);		
			set.add(v);
		}
		Vector<Vertex> vec = new Vector<Vertex>(set);
		return vec;
	}

}
