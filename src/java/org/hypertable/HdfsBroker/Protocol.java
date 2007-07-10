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

public class Protocol extends org.hypertable.AsyncComm.Protocol {

    public static final short COMMAND_OPEN     = 0;
    public static final short COMMAND_CREATE   = 1;
    public static final short COMMAND_CLOSE    = 2;
    public static final short COMMAND_READ     = 3;
    public static final short COMMAND_WRITE    = 4;
    public static final short COMMAND_SEEK     = 5;
    public static final short COMMAND_REMOVE   = 6;
    public static final short COMMAND_SHUTDOWN = 7;
    public static final short COMMAND_LENGTH   = 8;
    public static final short COMMAND_PREAD    = 9;
    public static final short COMMAND_MKDIRS   = 10;
    public static final short COMMAND_MAX      = 11;
    
    public static final short SHUTDOWN_FLAG_IMMEDIATE = 0x0001;

    public static String msCommandStrings[] = {
	"open",
	"create",
	"close",
	"read",
	"write",
	"seek",
	"remove",
	"shutdown",
	"length",
	"pread",
	"mkdirs"
    };

    public String CommandText(short command) {
	if (command < 0 || command >= COMMAND_MAX)
	    return "UNKNOWN (" + command + ")";
	return msCommandStrings[command];
    }

};
