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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.WritableComparable;

public class Membership implements WritableComparable<Membership> {

	private Collection<Vertex> members;
	
	public static Membership read(DataInput in) throws IOException {
		Membership read = new Membership();
		read.readFields(in);
		return read;
	}
	
	public Membership() {};
	
	@Override
  public void write(DataOutput out) throws IOException {
		if (members != null) {
			int howmany = members.size();
			out.writeInt(howmany);
			Iterator<Vertex> i = members.iterator();
			while(i.hasNext()) {
				Vertex m = i.next();
				m.write(out);
			}
		} else {
			out.writeInt(0);
		}
		
		
  }

	@Override
  public void readFields(DataInput in) throws IOException {

		int howmany = in.readInt();
		members = new ArrayList<Vertex>(howmany);
		while(howmany-- > 0) {
			Vertex m = Vertex.read(in);
			members.add(m);
		}
		
  }

	@Override
  public int compareTo(Membership o) {
	  int compareTo = 0;
		if (members != null) {
	  	if (o.members != null) {
	  		Iterator<Vertex> i = members.iterator();
	  		Iterator<Vertex> oi = o.members.iterator();
	  		while(true) {
	  			boolean n = i.hasNext();
	  			boolean on = i.hasNext();
	  			if(n && on) {
	  				// compare member by member
	  				compareTo = i.next().compareTo(oi.next());
	  				if (compareTo != 0) {
	  					return compareTo; // break on first difference
	  				} else {
	  					continue;
	  				}
	  			} else if (!n && on) {
	  				// we have fewer members than the other
	  				return Integer.MIN_VALUE;
	  			} else if (n && !on) {
	  				// the other has fewer members
	  				return Integer.MAX_VALUE;
	  			} else {
	  				return 0;
	  			}
	  		}
	  	} else {
	  		// the other has fewer
	  		return Integer.MAX_VALUE;
	  	}
	  } else {
	  	// we have no members
	  	if(o.members == null) {
	  		// both no members
	  		return 0;
	  	}else {
	  		if(o.members.size() > 0) {
	  			// we have fewer
	  			return Integer.MIN_VALUE;
	  		} else {
	  			// both no members
	  			return 0;
	  		}
	  	}
	  }
  }

	public void setMembers(Collection<Vertex> members) {
		this.members = members;
	}

	public Collection<Vertex> getMembers() {
		return members;
	}
	
	public Membership addMember(Vertex m) {
		if (members == null) {
			members = new TreeSet<Vertex>();
		}
		members.add(m);
		return this;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Membership) {
			return this.compareTo((Membership) o) == 0;
		} else {
			return false;
		}
	}
	
	@Override 
	public int hashCode() {
		int hash = 0;
		if(members != null) {
			Iterator<Vertex> i = members.iterator();
			hash = 1;
			while(i.hasNext()) {
				hash = hash * i.next().hashCode();
			}
		}
		return hash;
	}
	
}
