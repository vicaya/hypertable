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

package org.hypertable.Hyperspace;

public class Protocol extends org.hypertable.AsyncComm.Protocol {

    public static final short COMMAND_CREATE  = 0;
    public static final short COMMAND_DELETE  = 1;
    public static final short COMMAND_MKDIRS  = 2;
    public static final short COMMAND_ATTRSET = 3;
    public static final short COMMAND_ATTRGET = 4;
    public static final short COMMAND_ATTRDEL = 5;
    public static final short COMMAND_EXISTS  = 6;
    public static final short COMMAND_STATUS  = 7;
    public static final short COMMAND_MAX     = 8;

    public String CommandText(short command) {
	if (command < 0 || command >= COMMAND_MAX)
	    return "UNKNOWN (" + command + ")";
	return msCommandStrings[command];
    }

    public static String msCommandStrings[] = {
	"create",
	"delete",
	"mkdirs",
	"attrset",
	"attrget",
	"attrdel",
	"exists",
	"status"
    };
}
