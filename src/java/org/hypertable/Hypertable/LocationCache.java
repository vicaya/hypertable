/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package org.hypertable.Hypertable;

import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.TreeMap;

class LocationCache {

    public LocationCache(int maxEntries) {
	mMaxEntries = maxEntries;
    }

    private static class LocationKey implements Comparable<LocationKey> {
	public String table;
	public String endRow;

	/**
	 * This method compares the object against another LocationKey.  The thing
	 * to note here is that a null endRow implies the maximum value.
	 */
	public int compareTo(LocationKey other) {
	    int cmpval = table.compareTo(other.table);
	    if (cmpval != 0)
		return cmpval;
	    if (endRow == null) {
		if (other.endRow != null)
		    return 1;
	    }
	    else if (other.endRow == null)
		return -1;
	    else {
		if ((cmpval = endRow.compareTo(other.endRow)) != 0)
		    return cmpval;
	    }
	    return 0;
	}
    }

    private static class LocationDataNode {
	public LocationKey key;
	public String startRow;
	public InetSocketAddress location;
	public LocationDataNode prev;
	public LocationDataNode next;
    }

    public synchronized InetSocketAddress Lookup(String table, String rowKey) {
	LocationDataNode  node;
	LocationKey key = new LocationKey();
	key.endRow = rowKey;
	key.table = table;
	try {
	    key = mMap.tailMap(key).firstKey();
	    node = mMap.get(key);
	    if (node.startRow == null || rowKey.compareTo(node.startRow) > 0)
		return node.location;
	}
	catch (NoSuchElementException e) {
	}
	return null;
    }

    public synchronized void Insert(String table, String startRow, String endRow, InetSocketAddress location) {
	LocationDataNode  node;

	/**
	 *  Should we do a lookup first?
	 */ 

	/** remove oldest entry from head **/
	if (mNumEntries >= mMaxEntries) {
	    if (mHead.prev == null) {
		node = mHead;
		mHead = mTail = null;
	    }
	    else {
		node = mHead;
		mHead = mHead.prev;
		mHead.next = null;
	    }
	    node = mMap.remove(node.key);
	    mNumEntries--;
	}
	else {
	    node = new LocationDataNode();
	    node.key = new LocationKey();
	}

	node.key.table = table;
	node.key.endRow = endRow;
	node.startRow = startRow;
	node.location = location;

	/** Add new node to tail **/
	node.prev = null;
	node.next = mTail;
	if (mTail != null)
	    mTail.prev = node;
	mTail = node;

	/** Add node to map **/
	node = mMap.put(node.key, node);

	assert(node == null);
    }

    private TreeMap<LocationKey, LocationDataNode>  mMap = new TreeMap<LocationKey, LocationDataNode>();
    private LocationDataNode mHead = null;
    private LocationDataNode mTail = null;
    private int mNumEntries = 0;
    private int mMaxEntries;
}

