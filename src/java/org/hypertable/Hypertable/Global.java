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



package org.hypertable.Hypertable;

import java.util.HashMap;

import org.hypertable.AsyncComm.Comm;

public class Global {
    public static Comm            comm = null;
    public static boolean         verbose = false;
    public static boolean         interactive = true;
    public static org.hypertable.Hypertable.Master.Protocol  masterProtocol = new org.hypertable.Hypertable.Master.Protocol();
    public static org.hypertable.Hypertable.Master.Client    master;
    public static HashMap<String,Table>  tableMap = new HashMap<String,Table>();
    public static LocationCache    locationCache = new LocationCache(65536);
}
