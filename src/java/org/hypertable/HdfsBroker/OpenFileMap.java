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


package org.hypertable.HdfsBroker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 *
 */
public class OpenFileMap {

    public synchronized OpenFileData Create(int fd) {
	OpenFileData ofd = new OpenFileData();	
	mFileMap.put(fd, ofd);
	return ofd;
    }

    public synchronized OpenFileData Get(int fd) {
	return mFileMap.get(fd);
    }

    public synchronized OpenFileData Remove(int fd) {
	return mFileMap.remove(fd);
    }

    public synchronized void RemoveAll() {
	for (Iterator<Map.Entry<Integer,OpenFileData>> iter = mFileMap.entrySet().iterator(); iter.hasNext();) {
	    try {
		Map.Entry<Integer,OpenFileData> entry = iter.next();
		int id = entry.getKey();
		OpenFileData ofd = entry.getValue();
		if (ofd.is != null) {
		    if (Global.verbose)
			System.out.println("Closing input stream for file id " + id);
		    ofd.is.close();
		}
		else if (ofd.os != null) {
		    if (Global.verbose)
			System.out.println("Closing output stream for file id " + id);
		    ofd.os.close();
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

