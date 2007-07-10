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

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.LinkedList;
import java.util.logging.Logger;

import org.hypertable.Common.Error;

class Reactor implements Runnable {

    final Selector selector;
    final long EVENT_LOOP_TIMEOUT_MS = 5000;

    static final Logger log = Logger.getLogger("org.hypertable");

    LinkedList<IOHandler> pending = new LinkedList<IOHandler>();

    Reactor() throws IOException {
	selector = Selector.open();
    }

    public Selector GetSelector() { return selector; }

    public synchronized void AddToRegistrationQueue(IOHandler handler) {
	pending.add(handler);
    }

    public void WakeUp() {
	selector.wakeup();
    }

    public void Shutdown() {
	mShutdown = true;
	selector.wakeup();
    }

    private synchronized void DoRegistrations() {
	IOHandler handler;
	while (!pending.isEmpty()) {
	    handler = pending.removeFirst();
	    handler.Register(selector);
	}
    }

    public void run() {
	try {
	    Set selected;
	    while (!Thread.interrupted()) {
		selector.select(EVENT_LOOP_TIMEOUT_MS);
		if (mShutdown)
		    return;
		DoRegistrations();
		selected = selector.selectedKeys();
		Iterator iter = selected.iterator();
		while (iter.hasNext()) {
		    SelectionKey selkey = (SelectionKey)(iter.next());
		    IOHandler handler = (IOHandler)selkey.attachment();
		    handler.run(selkey);
		}
		selected.clear();
	    }
	}
	catch (IOException e) {
	    e.printStackTrace();
	}
    }

    void AddRequest(int id, IOHandlerData handler, CallbackHandler cb, long expire) {
	mRequestCache.Insert(id, handler, cb, expire);
    }

    CallbackHandler RemoveRequest(int id) {
	RequestCache.CacheNode node = mRequestCache.Remove(id);
	if (node != null)
	    return node.cb;
	return null;
    }

    void HandleTimeouts() {
	long now = System.currentTimeMillis();
	RequestCache.CacheNode node;
	while ((node = mRequestCache.GetNextTimeout(now)) != null) {
	    node.handler.DeliverEvent(  new Event(Event.Type.ERROR, node.handler.GetAddress(), Error.COMM_REQUEST_TIMEOUT, null), node.cb );
	}
    }

    void CancelRequests(IOHandlerData handler) {
	mRequestCache.PurgeRequests(handler);
    }

    private RequestCache mRequestCache = new RequestCache();
    private boolean mShutdown = false;
}
