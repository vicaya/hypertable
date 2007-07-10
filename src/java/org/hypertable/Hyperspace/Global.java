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

