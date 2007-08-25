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


package org.hypertable.DfsBroker.hadoop;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class acts as a synchronized HashMap that maps open file descriptors to
 * OpenFileData objects
 */
public class OpenFileMap {

    public synchronized OpenFileData Create(int fd, InetSocketAddress addr) {
	OpenFileData ofd = new OpenFileData();	
	ofd.addr = addr;
	mFileMap.put(fd, ofd);
	return ofd;
    }

    public synchronized OpenFileData Get(int fd) {
	return mFileMap.get(fd);
    }

    public synchronized OpenFileData Remove(int fd) {
	return mFileMap.remove(fd);
    }

    public synchronized void RemoveAll(InetSocketAddress addr) {
	for (Iterator<Map.Entry<Integer,OpenFileData>> iter = mFileMap.entrySet().iterator(); iter.hasNext();) {
	    try {
		Map.Entry<Integer,OpenFileData> entry = iter.next();
		int id = entry.getKey();
		OpenFileData ofd = entry.getValue();
		if (ofd.addr.equals(addr)) {
		    if (ofd.is != null) {
			ofd.is.close();
		    }
		    else if (ofd.os != null) {
			ofd.os.close();
		    }
		    iter.remove();
		}
	    }
	    catch (IOException e) {
		e.printStackTrace();
	    }
	}
	mFileMap.clear();
    }

    private HashMap<Integer, OpenFileData> mFileMap = new HashMap<Integer, OpenFileData>();
}

