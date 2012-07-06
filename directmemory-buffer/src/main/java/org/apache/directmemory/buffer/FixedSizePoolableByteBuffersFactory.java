package org.apache.directmemory.buffer;

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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.directmemory.memory.allocator.DirectByteBufferUtils;
import org.apache.directmemory.stream.ByteBufferStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * {@link PoolableByteBuffersFactory} implementation that instantiate {@link ByteBuffer}s of fixed size, called slices.
 * Highly inspired from Linux kernel SLAB's allocation.
 * 
 * @author bperroud
 * 
 * @since 0.2
 */
public class FixedSizePoolableByteBuffersFactory
	extends AbstractPoolableByteBuffersFactory
    implements PoolableByteBuffersFactory
{

    private static final long MAX_SEGMENT_SIZE = Integer.MAX_VALUE / 2;
    

    // Collection that keeps track of the parent buffers (segments) where slices are allocated
    private final List<ByteBuffer> segmentsBuffers;

    // Collection that owns all slices that can be used.
    private final Queue<ByteBuffer> freeBuffers = new ConcurrentLinkedQueue<ByteBuffer>();

    // Size of each slices dividing each segments of the slab
    private final int sliceSize;

    // Total size of the current slab
    private long totalSize;

    // Collection that keeps track of borrowed buffers
    private final Map<Integer, ByteBuffer> usedSliceBuffers = new ConcurrentHashMap<Integer, ByteBuffer>();



    /**
     * Constructor.
     *
     * @param number           : internal identifier of the allocator
     * @param totalSize        : the internal buffer
     * @param sliceSize        : arbitrary number of the buffer.
     * @param numberOfSegments : number of parent {@link ByteBuffer} to allocate.
     */
    public FixedSizePoolableByteBuffersFactory( final long totalSize, final int sliceSize,
                                             final int numberOfSegments )
    {

        this.sliceSize = sliceSize;

        this.segmentsBuffers = new ArrayList<ByteBuffer>( numberOfSegments );

        init( totalSize, numberOfSegments );

    }

    protected void init( long targetedTotalSize, int numberOfSegments )
    {
        checkArgument( numberOfSegments > 0 );

        // Compute the size of each segments. A segment can't be bigger than Integer.MAX_VALUE, 
        // so either numberOfSegments is given appropriately, or we force a bigger number of segments.
        int segmentSize = (targetedTotalSize / numberOfSegments) > MAX_SEGMENT_SIZE ? (int)MAX_SEGMENT_SIZE : (int)(targetedTotalSize / numberOfSegments);
        
        // size is rounded down to a multiple of the slice size
        segmentSize -= segmentSize % sliceSize;

        numberOfSegments = (int)(targetedTotalSize / segmentSize);
        
        long allocatedSize = 0;
        for ( int i = 0; i < numberOfSegments; i++ )
        {
            final ByteBuffer segment = ByteBuffer.allocateDirect( segmentSize );
            segmentsBuffers.add( segment );
            allocatedSize += segmentSize;
            for ( int j = 0; j < segment.capacity(); j += sliceSize )
            {
                segment.clear();
                segment.position( j );
                segment.limit( j + sliceSize );
                final ByteBuffer slice = segment.slice();
                freeBuffers.add( slice );
            }
            
            // set the position of the parent ByteBuffer to the end to avoid writing 
            // data directly inside.
            segment.position( segmentSize );
        }
        
        totalSize = allocatedSize;
    }



    
    protected ByteBuffer findFreeBuffer( )
    {
        // TODO : Add capacity to wait till a given timeout for a freed buffer
        return freeBuffers.poll();
    }

    @Override
    public void release( final ByteBuffer byteBuffer )
    {

        checkState( !isClosed() );

        if ( usedSliceBuffers.remove( DirectByteBufferUtils.getHash( byteBuffer ) ) == null )
        {
            throw new IllegalStateException( "This buffer has already been freeed" );
        }

        freeBuffers.offer( byteBuffer );

    }

    @Override
    public List<ByteBuffer> borrow( int size ) throws BufferOverflowException
    {

        checkState( !isClosed() );

        int numOfBBToAllocate = (int)Math.ceil( size / sliceSize );
        final List<ByteBuffer> byteBuffers = new ArrayList<ByteBuffer>(numOfBBToAllocate);
        
        int allocatedSize = 0;
        while (allocatedSize < size) {
            
            final ByteBuffer byteBuffer = findFreeBuffer();
            
            if (byteBuffer == null) {
                // free all borrowed buffers
                for (ByteBuffer alreadyBorrowedBuffer : byteBuffers) {
                    release( alreadyBorrowedBuffer );
                }
                throw new BufferOverflowException();
            }
            
            // Reset buffer's state
            byteBuffer.clear();
            byteBuffer.limit( Math.min( sliceSize, size - allocatedSize ) );

            usedSliceBuffers.put( DirectByteBufferUtils.getHash( byteBuffer ), byteBuffer );
            
            byteBuffers.add(byteBuffer);
            allocatedSize += sliceSize;
            
        }

        return byteBuffers;

    }

    public int getSliceSize()
    {
        return sliceSize;
    }

    @Override
    public void clear()
    {
        for ( final Map.Entry<Integer, ByteBuffer> entry : usedSliceBuffers.entrySet() )
        {
            freeBuffers.offer( entry.getValue() );
        }
        usedSliceBuffers.clear();
    }

    @Override
    public long getCapacity()
    {
        return totalSize;
    }

    @Override
    public void close()
    {
        checkState( !isClosed() );

        setClosed( true );

        clear();

        for ( final ByteBuffer buffer : segmentsBuffers )
        {
            try
            {
                DirectByteBufferUtils.destroyDirectByteBuffer( buffer );
            }
            catch ( Exception e )
            {
                getLogger().warn( "Exception thrown while closing the allocator", e );
            }
        }
    }
    

    @Override
    public ByteBufferStream getInOutStream()
    {
        return new ByteBufferStream( this );
    }

    
    @Override
    public int getDefaultAllocationSize()
    {
        return sliceSize;
    }
    
}
