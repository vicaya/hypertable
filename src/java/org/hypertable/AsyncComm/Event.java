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

import java.util.Date;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.hypertable.Common.Error;

public class Event {

    public static class Queue extends ConcurrentLinkedQueue<Event> {
	synchronized public void Add(Event event) {
	    add(event);
	    notify();
	}
	synchronized public Event Remove() throws InterruptedException {
	    while (isEmpty())
		wait();
	    Event event = peek();
	    remove(event);
	    return event;
	}
    }

    public enum Type { CONNECTION_ESTABLISHED, DISCONNECT, MESSAGE, ERROR }

    public Event(Event other) {
	type = other.type;
	addr = other.addr;
	error = other.error;
	msg = other.msg;
    }

    public Event(Type t, InetSocketAddress a, int err, Message m) {
	type = t;
	addr = a;
	error = err;
	msg = m;
    }

    public Event(Type t, InetSocketAddress a, int err) {
	type = t;
	addr = a;
	error = err;
	msg = null;
    }

    public String toString() {
	String str = "[" + addr + " ; " + (new Date()).toString() + "] ";
	if (type == Event.Type.CONNECTION_ESTABLISHED)
	    str += "Connection Established";
	else if (type == Event.Type.DISCONNECT)
	    str += "Disconnect";
	else if (type == Event.Type.MESSAGE)
	    str += "Message";
	else if (type == Event.Type.ERROR)
	    str += "Error";
	if (error != Error.OK) {
	    str += " - " + Error.GetText(error);
	}
	return str;
    }

    public Type type;
    public InetSocketAddress addr;
    public int    error;
    public Message msg;
}
