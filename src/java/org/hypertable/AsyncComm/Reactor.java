/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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

    void AddRequest(int id, IOHandlerData handler, DispatchHandler dh, long expire) {
	mRequestCache.Insert(id, handler, dh, expire);
    }

    DispatchHandler RemoveRequest(int id) {
	RequestCache.CacheNode node = mRequestCache.Remove(id);
	if (node != null)
	    return node.dh;
	return null;
    }

    void HandleTimeouts() {
	long now = System.currentTimeMillis();
	RequestCache.CacheNode node;
	while ((node = mRequestCache.GetNextTimeout(now)) != null) {
	    node.handler.DeliverEvent(  new Event(Event.Type.ERROR, node.handler.GetAddress(), Error.COMM_REQUEST_TIMEOUT, null), node.dh );
	}
    }

    void CancelRequests(IOHandlerData handler) {
	mRequestCache.PurgeRequests(handler);
    }

    private RequestCache mRequestCache = new RequestCache();
    private boolean mShutdown = false;
}
