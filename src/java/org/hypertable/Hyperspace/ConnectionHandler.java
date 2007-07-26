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

import java.net.ProtocolException;
import java.util.logging.Logger;

import org.hypertable.AsyncComm.CallbackHandler;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.HeaderBuilder;

import org.hypertable.Common.Error;


/**
 */
public class ConnectionHandler implements CallbackHandler {

    static final Logger log = Logger.getLogger("org.hypertable.Hyperspace");

    public void handle(Event event) {
	short command = -1;

	if (event.type == Event.Type.MESSAGE) {
	    //log.info(event.toString());
	    try {
		event.msg.buf.position(event.msg.headerLen);
		command = event.msg.buf.getShort();
		Global.workQueue.AddRequest( mRequestFactory.newInstance(event, command) );
	    }
	    catch (ProtocolException e) {
		HeaderBuilder hbuilder = new HeaderBuilder();
		CommBuf cbuf = Global.protocol.CreateErrorMessage(command, Error.PROTOCOL_ERROR, e.getMessage(), hbuilder.HeaderLength());

		// Encapsulate with Comm message response header
		hbuilder.LoadFromMessage(event.msg);
		hbuilder.Encapsulate(cbuf);

		Global.comm.SendResponse(event.addr, cbuf);
	    }
	}
	else
	    log.info(event.toString());
    }

    private RequestFactory mRequestFactory = new RequestFactory();
}
