package org.apache.directmemory.memory;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static java.lang.System.currentTimeMillis;
import static java.lang.String.format;

import java.nio.ByteBuffer;

public class PointerImpl<T>
    implements Pointer<T>
{
    public int start;

    public int end;

    public long created;

    public long expires;

    public long expiresIn;

    public long hits;

    public boolean free;

    public long lastHit;

    public int bufferNumber;

    public Class<? extends T> clazz;

    public ByteBuffer directBuffer = null;

    public PointerImpl()
    {
    }

    public PointerImpl( int start, int end )
    {
        this.start = start;
        this.end = end;
    }

    @Override
    public byte[] content()
    {
        return null;
    }

    @Override
    public float getFrequency()
    {
        return (float) ( currentTimeMillis() - created ) / hits;
    }

    @Override
    public int getCapacity()
    {
        return directBuffer == null ? end - start + 1 : directBuffer.limit();
    }

    @Override
    public String toString()
    {
        return format( "%s[%s, %s] %s free", getClass().getSimpleName(), start, end, ( free ? "" : "not" ) );
    }

    @Override
    public void reset()
    {
        free = true;
        created = 0;
        lastHit = 0;
        hits = 0;
        expiresIn = 0;
        clazz = null;
        directBuffer = null;
    }

    @Override
    public boolean isFree()
    {
        return free;
    }

    @Override
    public boolean isExpired()
    {
        if ( expires > 0 || expiresIn > 0 )
        {
            return ( expiresIn + created < currentTimeMillis() );
        }
        return false;
    }

    @Override
    public int getBufferNumber()
    {
        return bufferNumber;
    }

    @Override
    public int getStart()
    {
        return start;
    }

    @Override
    public int getEnd()
    {
        return end;
    }

    @Override
    public void setStart( int start )
    {
        this.start = start;
    }

    @Override
    public void hit()
    {
        lastHit = System.currentTimeMillis();
        hits++;
    }

    @Override
    public Class<? extends T> getClazz()
    {
        return clazz;
    }

    @Override
    public ByteBuffer getDirectBuffer()
    {
        return directBuffer;
    }

    @Override
    public void setFree( boolean free )
    {
        this.free = free;
    }

    @Override
    public void setEnd( int end )
    {
        this.end = end;
    }

    @Override
    public void setClazz( Class<? extends T> clazz )
    {
        this.clazz = clazz;
    }

    @Override
    public void setDirectBuffer( ByteBuffer directBuffer )
    {
        this.directBuffer = directBuffer;
    }

    @Override
    public void createdNow()
    {
        created = System.currentTimeMillis();
    }

    @Override
    public void setBufferNumber( int bufferNumber )
    {
        this.bufferNumber = bufferNumber;
    }

    @Override
    public void setExpiration( long expires, long expiresIn )
    {
        this.expires = expires;
        this.expiresIn = expiresIn;
    }
}
