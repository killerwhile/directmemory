package org.apache.directmemory.stream;

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

import java.io.Closeable;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.directmemory.buffer.PoolableByteBuffersFactory;

public class ByteBufferStream
    implements Closeable
{

    private static final int DEFAULT_INITIAL_SIZE = -1;

    static final long EXPIRATION = -1L;

    private final PoolableByteBuffersFactory factory;

    private final List<ByteBuffer> byteBuffers = new ArrayList<ByteBuffer>();

    private final List<Integer> byteBufferStartPositions = new ArrayList<Integer>();

    private final int allocationSize;

    private final ByteBufferOutputStream offHeapOutputStream;

    private final List<ByteBufferInputStream> offHeapInputStreams = new ArrayList<ByteBufferInputStream>();

    public ByteBufferStream( final PoolableByteBuffersFactory factory )
    {
        this( factory, DEFAULT_INITIAL_SIZE, factory.getDefaultAllocationSize() );
    }

    public ByteBufferStream( final PoolableByteBuffersFactory factory, final int intialSize, final int allocationSize )
    {
        this.factory = factory;
        this.allocationSize = allocationSize;

        offHeapOutputStream = new ByteBufferOutputStream( this );

        if (intialSize > 0)
        {
            allocate( intialSize );
        }
    }

    void allocate( int minWritableBytes ) {
        // Borrow at least minWritableBytes, but in multiple of allocationSize.
        int sizeToAllocate = (int)Math.ceil((double)minWritableBytes / allocationSize ) * allocationSize;
        final List<ByteBuffer> freshBuffers = factory.borrow( sizeToAllocate );

        for (ByteBuffer freshByteBuffer : freshBuffers) {
            int previousByteBufferLimit = byteBuffers.size() == 0 ? 0 : byteBuffers.get( byteBuffers.size() - 1 ).limit();
            int previousStartPositition = byteBufferStartPositions.size() == 0 ? 0 : byteBufferStartPositions.get( byteBufferStartPositions.size() - 1 );

            // Stream want to use th full capacity of the buffer.
            freshByteBuffer.clear();

            byteBuffers.add( freshByteBuffer );
            // compute buffers start's position : previous startPosition + previous buffer's capacity
            byteBufferStartPositions.add( previousStartPositition + previousByteBufferLimit );
        }

        for (final ByteBufferInputStream in : offHeapInputStreams) {
            in.addByteBuffers( freshBuffers );
        }
    }


    List<ByteBuffer> getByteBuffers() {
        return byteBuffers;
    }


    List<Integer> getByteBufferStartPositions() {
        return byteBufferStartPositions;
    }


    public ByteBufferInputStream getInputStream()
    {
        ByteBufferInputStream bbis = new ByteBufferInputStream( this );
        offHeapInputStreams.add(bbis);
        return bbis;
    }

    public ByteBufferOutputStream getOutputStream()
    {
        return offHeapOutputStream;
    }

    @Override
    public void close()
        throws IOException
    {
        // TODO Auto-generated method stub

    }

    void outputStreamClosed() {

    }

    void inputStreamClosed() {

    }


    ByteBufferStreamOffset computeOffset(int offset) throws BufferOverflowException
    {

        int i = Collections.binarySearch( byteBufferStartPositions, Integer.valueOf( offset ) );

        final ByteBufferStreamOffset o = new ByteBufferStreamOffset();
        if (i >= 0) {
            o.bufferIndex = i;
            o.bufferPosition = 0;
        } else {
            i = -i - 1;
            if (i == byteBufferStartPositions.size()) {
                throw new BufferOverflowException();
            }
            o.bufferIndex = i;
            o.bufferPosition = offset - byteBufferStartPositions.get( i ) ;
        }
        return o;
    }

    public int getCurrentCapacity() {
        if (byteBuffers.size() > 0) {
            int lastIndex = byteBuffers.size() - 1;
            return byteBufferStartPositions.get(lastIndex) + byteBuffers.get(lastIndex).limit();
        } else {
            return 0;
        }
    }
}
