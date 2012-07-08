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
    
    // Collection that keeps track of the parent buffers (segments) where slices are allocated
    private final List<ByteBuffer> segmentsBuffers;

    // Collection that owns all slices that can be used.
    private final Queue<ByteBuffer> freeBuffers = new ConcurrentLinkedQueue<ByteBuffer>();

    // Size of each slices dividing each segments of the slab
    private final int sliceSize;

    // Total size of the current slab
    private final long totalSize;

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
                                             int numberOfSegments )
    {

        this.sliceSize = sliceSize;

        checkArgument( numberOfSegments > 0 );

        // Compute the size of each segments. A segment can't be bigger than Integer.MAX_VALUE / 2, 
        // so either numberOfSegments is given appropriately, or we force a bigger number of segments.
        int segmentSize = (totalSize / numberOfSegments) > MAX_SEGMENT_SIZE ? (int)MAX_SEGMENT_SIZE : (int)(totalSize / numberOfSegments);
        
        // size is rounded down to a multiple of the slice size
        segmentSize -= segmentSize % sliceSize;

        // Recompute the number of segments. Can be changed if the segmentSize has been changed.
        numberOfSegments = (int)(totalSize / segmentSize);

        this.segmentsBuffers = new ArrayList<ByteBuffer>( numberOfSegments );

        long allocatedSize = 0;
        
        // Create all parents segments.
        for ( int i = 0; i < numberOfSegments; i++ )
        {
            final ByteBuffer segment = ByteBuffer.allocateDirect( segmentSize );
            segmentsBuffers.add( segment );
            allocatedSize += segmentSize;
            
            // Create all slabs
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
        
        this.totalSize = allocatedSize;

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

        // How many slabs to allocate to succeed this request
        int numOfBBToAllocate = (int)Math.ceil( (double)size / sliceSize );
        
        final List<ByteBuffer> byteBuffers = new ArrayList<ByteBuffer>(numOfBBToAllocate);
        
        ByteBuffer lastByteBuffer = null;
        
        for (int i = 0; i < numOfBBToAllocate; i++) {
            
            final ByteBuffer byteBuffer = findFreeBuffer();
            
            if (byteBuffer == null) {
                // free all borrowed buffers
                for (ByteBuffer alreadyBorrowedBuffer : byteBuffers) {
                    release( alreadyBorrowedBuffer );
                }
                throw new BufferOverflowException();
            }
            
            byteBuffers.add(byteBuffer);

            // Reset buffer's state
            byteBuffer.clear();
            
            // Keep track of the borrowed buffer
            usedSliceBuffers.put( DirectByteBufferUtils.getHash( byteBuffer ), byteBuffer );
            
            lastByteBuffer = byteBuffer;
        }

        if (lastByteBuffer != null) {
        	// Set the limit, can be partial if the requested allocation size is not
        	// a multiple of sliceSize
        	lastByteBuffer.limit( Math.min( sliceSize, size - (numOfBBToAllocate - 1) * sliceSize) );
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
    	// Re-add all borrowed buffer into the free buffer's queue.
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

        // destroy parents segments.
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
