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

import java.util.Date;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.hypertable.Common.Error;

public class Event {

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
