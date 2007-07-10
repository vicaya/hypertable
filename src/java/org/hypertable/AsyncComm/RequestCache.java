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


package org.hypertable.AsyncComm;

import java.util.HashMap;

class RequestCache {

    public static class CacheNode {
	CacheNode       prev;
	CacheNode       next;
	long            expire;
	int             id;
	IOHandlerData   handler;
	CallbackHandler cb;
    }

    public synchronized void Insert(int id, IOHandlerData handler, CallbackHandler cb, long expire) {

	CacheNode node = new CacheNode();

	node.id = id;
	node.handler = handler;
	node.cb = cb;
	node.expire = expire;

	if (mHead == null) {
	    node.next = node.prev = null;
	    mHead = mTail = node;
	}
	else {
	    node.next = mTail;
	    node.next.prev = node;
	    node.prev = null;
	    mTail = node;
	}

	mIdMap.put(id, node);
    }


    public synchronized CacheNode Remove(int id) {

	CacheNode node = mIdMap.remove(id);

	if (node == null) {
	    //LOG_VA_DEBUG("ID %d not found in request cache", id);
	    return null;
	}

	if (node.prev == null)
	    mTail = node.next;
	else
	    node.prev.next = node.next;

	if (node.next == null)
	    mHead = node.prev;
	else
	    node.next.prev = node.prev;

	if (node.handler != null)
	    return node;

	return null;
    }


    public synchronized CacheNode GetNextTimeout(long now)  {

	while (mHead != null && mHead.expire < now) {
	    CacheNode node = mIdMap.remove(mHead.id);
	    if (node != mHead)
		throw new AssertionError("node != mHead");
	    node = mHead;
	    if (mHead.prev != null) {
		mHead = mHead.prev;
		mHead.next = null;
	    }
	    else
		mHead = mTail = null;

	    if (node.handler != null)
		return node;
	}
	return null;
    }

    public synchronized void PurgeRequests(IOHandlerData handler) {
	for (CacheNode node = mTail; node != null; node = node.next) {
	    if (node.handler == handler) {
		//LOG_VA_DEBUG("Purging request id %d", node.id);
		node.handler = null;  // mark for deletion
	    }
	}
    }

    private HashMap<Integer, CacheNode>  mIdMap = new HashMap<Integer, CacheNode>();
    private CacheNode mHead, mTail;

}


