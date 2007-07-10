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


package org.hypertable.AsyncComm;

import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;

public class MessageBuilderSimple extends MessageBuilder {

    public void Reset(byte protocol) {
	mId = msNextId.incrementAndGet();
	mProtocol = protocol;
    }

    public void Reset(byte protocol, byte flags) {
	mId = msNextId.incrementAndGet();
	mProtocol = protocol;
	mFlags = flags;
    }

    public void LoadFromMessage(Message msg) {
	mId = msg.id;
	mProtocol = msg.protocol;
	mFlags = msg.flags;
    }

    public int HeaderLength() {
	return Message.HEADER_LENGTH;
    }

    public void Encapsulate(CommBuf cbuf) {
	cbuf.data.position(cbuf.data.position()-Message.HEADER_LENGTH);
	int totalLen = cbuf.data.remaining();
	if (cbuf.ext != null)
	    totalLen += cbuf.ext.remaining();
	cbuf.data.mark();
	cbuf.data.put(Message.VERSION);
	cbuf.data.put(mProtocol);
	cbuf.data.put(mFlags);
	cbuf.data.put(Message.HEADER_LENGTH);
	cbuf.data.putInt(mId);
	cbuf.data.putInt(totalLen);
	cbuf.data.reset();
	cbuf.id = mId;
    }

}


