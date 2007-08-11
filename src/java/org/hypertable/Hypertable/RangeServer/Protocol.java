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

package org.hypertable.Hypertable.RangeServer;

import java.io.UnsupportedEncodingException;

import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.Message;
import org.hypertable.AsyncComm.HeaderBuilder;

public class Protocol extends org.hypertable.AsyncComm.Protocol {

    /**
     * Builds a LOAD RANGE request
     */
    public CommBuf BuildRequestLoadRange(RangeSpecification rangeSpec) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 2 + rangeSpec.SerializedLength());

	rangeSpec.Prepend(cbuf);
	cbuf.PrependShort(COMMAND_LOAD_RANGE);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    /**
     * Builds an UPDATE request
     */
    public CommBuf BuildRequestUpdate(RangeSpecification rangeSpec, byte [] mods) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 2 + rangeSpec.SerializedLength() + 4 + mods.length);

	cbuf.PrependByteArray(mods);
	rangeSpec.Prepend(cbuf);
	cbuf.PrependShort(COMMAND_UPDATE);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    /**
     * Builds a CREATE SCANNER request
     */
    public CommBuf BuildRequestCreateScanner(RangeSpecification rangeSpec, ScanSpecification scanSpec) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 2 + rangeSpec.SerializedLength() + scanSpec.SerializedLength());

	scanSpec.Prepend(cbuf);
	rangeSpec.Prepend(cbuf);
	cbuf.PrependInt(tableGeneration);
	cbuf.PrependShort(COMMAND_CREATE_SCANNER);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }


    /**
     * Builds a FETCH SCANBLOCK request
     */
    public CommBuf BuildRequestFetchScanblock(int scannerId) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 6);
	cbuf.PrependInt(scannerId);
	cbuf.PrependShort(COMMAND_FETCH_SCANBLOCK);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }


    /**
     * Builds a COMPACT request
     */
    public CommBuf BuildRequestCompact(RangeSpecification rangeSpec, boolean major) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 2 + rangeSpec.SerializedLength() + 1);

	cbuf.PrependByte(major ? (byte)1 : (byte)0);
	rangeSpec.Prepend(cbuf);
	cbuf.PrependShort(COMMAND_COMPACT);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    public CommBuf BuildRequestBadCommand(short command) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 2);
	cbuf.PrependShort(command);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    public static final short COMMAND_LOAD_RANGE     = 0;
    public static final short COMMAND_UPDATE          = 1;
    public static final short COMMAND_CREATE_SCANNER  = 2;
    public static final short COMMAND_FETCH_SCANBLOCK = 3;
    public static final short COMMAND_COMPACT         = 4;
    public static final short COMMAND_MAX             = 5;

    private static String msCommandStrings[] = {
	"load range",
	"update",
	"create scanner",
	"fetch scanblock",
	"compact"
    };
    
    public String CommandText(short command) {
	if (command < 0 || command >= COMMAND_MAX)
	    return "UNKNOWN (" + command + ")";
	return msCommandStrings[command];
    }

}

