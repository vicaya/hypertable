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

import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;

public class ReactorFactory {

    public static void Initialize(short count) throws IOException {
	reactors = new Reactor [count];
	for (int i=0; i<count; i++) {
	    reactors[i] = new Reactor();
	    Thread thread = new Thread(reactors[i], "Reactor " + i);
	    thread.setPriority(Thread.MAX_PRIORITY);
	    thread.start();
	}
    }

    public static void Shutdown() {
	for (int i=0; i<reactors.length; i++)
	    reactors[i].Shutdown();
    }

    public static Reactor Get() {
	return reactors[nexti.getAndIncrement() % reactors.length];
    }

    private static AtomicInteger nexti = new AtomicInteger(0);

    private static Reactor [] reactors;
}

