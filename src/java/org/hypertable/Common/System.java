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


package org.hypertable.Common;


import java.io.File;
import java.util.StringTokenizer;


public class System {

    static {
	String classpath = java.lang.System.getProperty("java.class.path");
	
	StringTokenizer st = new StringTokenizer(classpath, ":");
	while (st.hasMoreTokens()) {
	    String path = st.nextToken();
	    if (!path.endsWith(".jar")) {
		File base = new File(path);
		File binDir = new File(base, "bin");
		if (!binDir.exists())
		    continue;
		File confDir = new File(base, "conf");
		if (!confDir.exists())
		    continue;
		if (path.endsWith("/"))
		    installDir = path.substring(0, path.length()-1);
		else
		    installDir = path;
		break;
	    }
	}
	
	// Figure out the number of cpus
	processorCount = Runtime.getRuntime().availableProcessors();
    }

    public static String installDir;

    public static int processorCount;

    public static void main(String [] args) {
	java.lang.System.out.println("Installation Directory = '" + installDir + "'");
    }
    

}

