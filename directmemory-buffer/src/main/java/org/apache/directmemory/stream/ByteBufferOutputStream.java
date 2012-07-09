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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

public class ByteBufferOutputStream
    extends OutputStream
{

    private final ByteBufferStream offHeapStream;

    private final List<ByteBuffer> byteBuffers;

    private final List<Integer> byteBufferStartPositions;

    private final ByteBufferStreamOffset currentOffset = new ByteBufferStreamOffset();


    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ByteBufferOutputStream( final ByteBufferStream offHeapStream )
    {
        this.offHeapStream = offHeapStream;
        this.byteBuffers = offHeapStream.getByteBuffers();
        this.byteBufferStartPositions = offHeapStream.getByteBufferStartPositions();
    }

    @Override
    public void flush()
        throws IOException
    {
        // noop
    }

    @Override
    public void close()
        throws IOException
    {
        if (closed.compareAndSet( false, true )) {
            offHeapStream.outputStreamClosed();
        }
    }

    @Override
    public void write( final byte[] b, final int off, final int len )
    {
        Preconditions.checkPositionIndex( off + len, b.length );
        Preconditions.checkState( !isClosed() );

        int remainingBytes = len;
        int internalOffset = off;

        ensureWritableBytes( len );

        while (remainingBytes > 0)
        {
            final ByteBuffer byteBuffer = getCurrentByteBuffer();

            int bytesToWrite = Math.min( getCurrentRemainingBytes(), remainingBytes );

            byteBuffer.put( b, internalOffset, bytesToWrite );

            remainingBytes -= bytesToWrite;
            internalOffset += bytesToWrite;

            incrementedOffset( bytesToWrite );
        }
    }


    /**
     * Ensure that at least minWritableBytes of writable bytes are available to be written.
     * Allocate more {@link ByteBuffer}s from the factory otherwise.
     *
     * @param minWritableBytes
     */
    private void ensureWritableBytes(int minWritableBytes) {

        int remainingBytes = getRemainingWritableBytes();

        if ( remainingBytes < minWritableBytes  ) {
            offHeapStream.allocate( minWritableBytes - remainingBytes );
        }
    }



    private int getRemainingWritableBytes() {

        if (currentOffset.bufferIndex >= byteBuffers.size() || currentOffset.bufferIndex >= byteBufferStartPositions.size()) {
            return 0;
        }

        int remainingWritableBytes = byteBufferStartPositions.get(byteBufferStartPositions.size() - 1) + byteBuffers.get( byteBuffers.size() - 1 ).limit()
            - (byteBufferStartPositions.get(currentOffset.bufferIndex) + currentOffset.bufferPosition);

        return remainingWritableBytes;
    }




    int getCurrentPosition() {
        int bufferSize = byteBuffers.size();
        if (bufferSize > 0) {
            int index = currentOffset.bufferIndex;
            if (index >= bufferSize) {
                return byteBufferStartPositions.get(bufferSize - 1) + byteBuffers.get( bufferSize - 1).limit();
            }
            else {
                return byteBufferStartPositions.get(index) + currentOffset.bufferPosition;
            }
        } else {
            return 0;
        }
    }

    private ByteBuffer getCurrentByteBuffer() {
        return byteBuffers.get(currentOffset.bufferIndex);
    }


    private void incrementedOffset( int increment )
    {
        Preconditions.checkArgument( increment >= 0 );
        while (increment > 0) {
            final int remaining = getCurrentRemainingBytes();
            if (increment < remaining) {
                currentOffset.bufferPosition += increment;
            } else {
                currentOffset.bufferIndex += 1;
                currentOffset.bufferPosition = 0;
            }
            increment -= remaining;
        }
    }

    private int getCurrentRemainingBytes() {
        if (byteBuffers.size() > 0) {
            return getCurrentByteBuffer().limit() - currentOffset.bufferPosition;
        } else {
            return 0;
        }
    }

    @Override
    public void write( final byte[] b )
    {
        write( b, 0, b.length );
    }

    @Override
    public void write( final int arg0 )
    {
        final byte b = (byte)arg0;
        byte[] wrappedByte = new byte[1];
        wrappedByte[0] = b;
        write( wrappedByte, 0, 1 );
    }

    boolean isClosed()
    {
        return closed.get();
    }

}
