/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

package org.hypertable.DfsBroker.hadoop;

import java.net.ProtocolException;
import java.util.logging.Logger;
import org.hypertable.AsyncComm.ApplicationHandler;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommHeader;
import org.hypertable.AsyncComm.Event;
import org.hypertable.Common.Error;
import org.hypertable.Common.System;


public class RequestHandlerWrite extends ApplicationHandler {

    static final Logger log = Logger.getLogger(
        "org.hypertable.DfsBroker.hadoop");

    public RequestHandlerWrite(Comm comm, HdfsBroker broker, Event event) {
        super(event);
        mComm = comm;
        mBroker = broker;
    }

    public void run() {
        int     fd, amount;
        boolean sync;
        ResponseCallbackWrite cb = new ResponseCallbackWrite(mComm, mEvent);

        try {

            if (mEvent.payload.remaining() < 8)
                throw new ProtocolException("Truncated message");

            fd = mEvent.payload.getInt();
            amount = mEvent.payload.getInt();
            sync = mEvent.payload.get() != 0;

            mEvent.payload.position(mEvent.payload.position() +
                                    (System.HT_DIRECT_IO_ALIGNMENT-9));

            byte [] data = new byte [ amount ];
            mEvent.payload.get(data);

            mBroker.Write(cb, fd, amount, data, sync);
        }
        catch (ProtocolException e) {
            int error = cb.error(Error.PROTOCOL_ERROR, e.getMessage());
            log.severe("Protocol error (WRITE) - " + e.getMessage());
            if (error != Error.OK)
                log.severe("Problem sending (WRITE) error back to client - "
                           + Error.GetText(error));
        }
    }

    private Comm       mComm;
    private HdfsBroker mBroker;
}
