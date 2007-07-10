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

public abstract class MessageBuilder {

    protected static AtomicInteger msNextId = new AtomicInteger();

    public abstract void Reset(byte protocol);

    public abstract void Reset(byte protocol, byte flags);

    public abstract void LoadFromMessage(Message msg);

    public abstract int HeaderLength();

    public abstract void Encapsulate(CommBuf cbuf);

    public void SetFlags(byte flags) { mFlags = flags; }

    public void AddFlag(byte flag) { mFlags |= flag; }

    public void SetProtocol(byte protocol) { mProtocol = protocol; }

    protected int mId;
    protected byte mProtocol;
    protected byte mFlags;
}

