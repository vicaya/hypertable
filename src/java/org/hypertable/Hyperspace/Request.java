/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package org.hypertable.Hyperspace;

import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.hypertable.AsyncComm.ApplicationHandler;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.HeaderBuilder;

public abstract class Request extends ApplicationHandler {

    static final Logger log = Logger.getLogger("org.hypertable.Hyperspace");

    public Request(Event event) {
	super(event);
    }

    //public abstract void run();

    protected HeaderBuilder mHeaderBuilder = new HeaderBuilder();
}
