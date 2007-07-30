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

import org.hypertable.AsyncComm.Comm;
import org.hypertable.Common.WorkQueue;

public class Global {
    public static Comm       comm = null;
    public static LockMap    lockMap = new LockMap();
    public static WorkQueue  workQueue = null;
    public static String     baseDir = null;
    public static Protocol   protocol = null;
    public static boolean    verbose = false;
}

