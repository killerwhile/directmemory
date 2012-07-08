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
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

public class ByteBufferInputStream
    extends InputStream
{

    private final ByteBufferStream offHeapStream;
    
    private final List<ByteBuffer> byteBuffers = new ArrayList<ByteBuffer>();

    private final List<Integer> byteBufferStartPositions;
        
    private final ByteBufferStreamOffset currentOffset = new ByteBufferStreamOffset();

    private final ByteBufferStreamOffset markedOffset = new ByteBufferStreamOffset();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    public ByteBufferInputStream( final ByteBufferStream offHeapStream )
    {
        this.offHeapStream = offHeapStream;
        this.byteBufferStartPositions = offHeapStream.getByteBufferStartPositions();
        addByteBuffers( offHeapStream.getByteBuffers() );
    }
    
    void addByteBuffers(List<ByteBuffer> bytesBuffers) {
        for (final ByteBuffer byteBuffer : bytesBuffers) {
            final ByteBuffer roBB = byteBuffer.asReadOnlyBuffer();
            roBB.rewind();
            this.byteBuffers.add(roBB);
        }
    }
    
    
    @Override
    public int available()
        throws IOException
    {
        return offHeapStream.getOutputStream().getCurrentPosition() - getCurrentPosition();
    }

    @Override
    public void close()
        throws IOException
    {
        if (closed.compareAndSet( false, true )) {
            offHeapStream.inputStreamClosed();
        }
    }

    @Override
    public synchronized void mark( int readlimit )
    {
    	 if (offHeapStream.getOutputStream().getCurrentPosition() < readlimit) {
             throw new BufferOverflowException();
         }
    	 if (offHeapStream.getCurrentCapacity() < readlimit) {
             throw new BufferOverflowException();
         }
        ByteBufferStreamOffset mark = offHeapStream.computeOffset( readlimit );
        this.markedOffset.bufferIndex = mark.bufferIndex;
        this.markedOffset.bufferPosition = mark.bufferPosition;
    }

    @Override
    public boolean markSupported()
    {
        return true;
    }

    
    protected void setOffset( int offset )
    {
    	 if (offHeapStream.getOutputStream().getCurrentPosition() < offset) {
             throw new BufferOverflowException();
         }
    	 if (offHeapStream.getCurrentCapacity() < offset) {
             throw new BufferOverflowException();
         }
        ByteBufferStreamOffset newOffset = offHeapStream.computeOffset( offset );
        this.currentOffset.bufferIndex = newOffset.bufferIndex;
        this.currentOffset.bufferPosition = newOffset.bufferPosition;
    }
    
    @Override
    public int read( byte[] b, int off, int len )
        throws IOException
    {
        
        Preconditions.checkPositionIndex( off + len, b.length );
        Preconditions.checkState( !isClosed() );
        
        int remainingBytes = len;
        int internalOffset = 0;
        
        while (remainingBytes > 0)
        {
            final ByteBuffer byteBuffer = getCurrentByteBuffer();
            
            if (byteBuffer == null) {
                break;
            }
            
            int bytesToRead = Math.min( Math.min(available(), getCurrentRemainingBytes()), remainingBytes );
            
            byteBuffer.get( b, internalOffset + off, bytesToRead );
            
            remainingBytes -= bytesToRead;
            internalOffset += bytesToRead;
            
            incrementedOffset( bytesToRead );
            
            if (bytesToRead <= 0) {
                break;
            }
        }
        
        return internalOffset;
    }

    @Override
    public int read( byte[] b )
        throws IOException
    {
        return read( b, 0, b.length );
    }

    @Override
    public int read()
        throws IOException
    {
        final byte[] b = new byte[1];
        int i = read(b);
        if (i > 0) {
            return (int)b[0];
        } else {
            return -1;
        }
    }
    
    public byte readByte(int offset) {
    	setOffset(offset);
    	try {
    		final byte[] b = new byte[1];
            int i = read(b);
            
            if (i > 0) {
                return b[0];
            } else {
                return -1;
            }
            
    	} catch (IOException e) {
    		throw new IndexOutOfBoundsException(e.getMessage());
    	}
    }
    
    public int readBytes(int offset, byte[] bytes, int o, int len) {
    	setOffset(offset);
    	try {

            int i = read(bytes, o, len);
            
            incrementedOffset(i);

            return i;
            
    	} catch (IOException e) {
    		throw new IndexOutOfBoundsException(e.getMessage());
    	}
    }
    
    @Override
    public synchronized void reset()
        throws IOException
    {
        currentOffset.bufferIndex = 0;
        currentOffset.bufferPosition = 0;
    }

    @Override
    public long skip( long n )
        throws IOException
    {
        if (getCurrentPosition() + n > offHeapStream.getOutputStream().getCurrentPosition()) {
            throw new BufferOverflowException();
        }
        incrementedOffset( (int)n );
        return n;
    }

    boolean isClosed()
    {
        return closed.get();
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
                int limit = getCurrentByteBuffer().limit();
                if (limit < currentOffset.bufferPosition) {
                    currentOffset.bufferIndex += 1;
                    currentOffset.bufferPosition -= limit;
                }
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
    
}
