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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.directmemory.memory.allocator.DirectByteBufferUtils;
import org.apache.directmemory.stream.ByteBufferStream;

import static com.google.common.base.Preconditions.checkState;

/**
 * {@link PoolableByteBuffersFactory} implementation that instantiate {@link ByteBuffer}s. 
 * Allocation algorithm is based on Knuth's bubby allocation scheme. 
 *
 * @since 0.2
 */
public class MergingBubbyPoolableByteBuffersFactory
    extends AbstractPoolableByteBuffersFactory
    implements PoolableByteBuffersFactory
{

    private static final long MAX_SEGMENT_SIZE = Integer.MAX_VALUE / 2;

    private static final int DEFAULT_MIN_ALLOCATION_SIZE = 128;

    // Collection that keeps track of the parent buffers (segments) where slices are allocated
    private final List<ByteBuffer> segmentsBuffers;

    // Size of each slices dividing each segments of the slab
    private final int minAllocationSize;

    private final int maxLevel;
    private final int segmentSizeLevel;
    private final int segmentSize;
    
    // Total size of the current slab
    private long totalSize;

    private final LinkedHashQueue<LinkedByteBuffer>[] freeBuffers;

    private final Map<Integer, LinkedByteBuffer> borrowedBuffers = new ConcurrentHashMap<Integer, LinkedByteBuffer>();


    /**
     * Constructor.
     *
     * @param totalSize         : the total  to allocate
     * @param numberOfSegments  : number of parent {@link ByteBuffer} to allocate.
     */
    public MergingBubbyPoolableByteBuffersFactory( final long totalSize, final int numberOfSegments )
    {
        this( totalSize, numberOfSegments, DEFAULT_MIN_ALLOCATION_SIZE );
    }
    
    /**
     * Constructor.
     *
     * @param totalSize         : the total  to allocate
     * @param numberOfSegments  : number of parent {@link ByteBuffer} to allocate.
     * @param minAllocationSize : minimal size to allocate.  Allocation request for smaller size will not split the {@link ByteBuffer}.
     */
	public MergingBubbyPoolableByteBuffersFactory( final long totalSize, int numberOfSegments, final int minAllocationSize )
    {

        this.minAllocationSize = minAllocationSize;
        
        this.segmentsBuffers = new ArrayList<ByteBuffer>( numberOfSegments );


        // Compute the size of each segments. A segment can't be bigger than Integer.MAX_VALUE, 
        // so either numberOfSegments is given appropriately, or we force a bigger number of segments.
        int segmentSize = (totalSize / numberOfSegments) > MAX_SEGMENT_SIZE ? (int)MAX_SEGMENT_SIZE : (int)(totalSize / numberOfSegments);
        
        // segmentSize must be a power of two
        checkState((segmentSize & (segmentSize-1)) == 0 );

        // minAllocationSize must be a power of two
        checkState((minAllocationSize & (minAllocationSize-1)) == 0 );
        
        numberOfSegments = (int)(totalSize / segmentSize);
        
        checkState( numberOfSegments > 0 );
        
        this.segmentSizeLevel = minExponantOf2( segmentSize );
        this.maxLevel = segmentSizeLevel - minExponantOf2 ( minAllocationSize ) + 1;
        this.segmentSize = segmentSize;
        
        freeBuffers = new LinkedHashQueue[maxLevel];
        
        for (int i = 0; i < maxLevel; i++) {
            freeBuffers[i] = new LinkedHashQueue<LinkedByteBuffer>();
        }
        
        long allocatedSize = 0;
        for ( int i = 0; i < numberOfSegments; i++ )
        {
            final ByteBuffer segment = ByteBuffer.allocateDirect( segmentSize );
            segmentsBuffers.add( segment );
                        
            freeBuffers[0].offer( new LinkedByteBuffer( 0, segment ) );
            
            allocatedSize += segmentSize;
        }
        
        this.totalSize = allocatedSize;
    }

    private static int minExponantOf2(int i ) {
        return (int)Math.ceil( Math.log( i ) / Math.log( 2 ) );
    }
    
    private static int maxExponantOf2(int i ) {
        return (int)Math.floor( Math.log( i ) / Math.log( 2 ) );
    }

    @Override
    public void release( final ByteBuffer byteBuffer )
    {

        checkState( !isClosed() );

        LinkedByteBuffer linkedByteBuffer = borrowedBuffers.remove( DirectByteBufferUtils.getHash( byteBuffer ) );
            
        if ( linkedByteBuffer == null )
        {
            throw new IllegalStateException( "This buffer has already been freeed" );
        }

        merge(linkedByteBuffer);
    }
    
    
    private void merge(LinkedByteBuffer linkedByteBuffer) {
        while (linkedByteBuffer.level > 0 && linkedByteBuffer.bubby.free) {
            linkedByteBuffer.free = false;
            
            freeBuffers[ linkedByteBuffer.level ].remove( linkedByteBuffer.bubby );
            
            linkedByteBuffer.bubby.free = false;
            
            LinkedByteBuffer parent = linkedByteBuffer.parent;
            linkedByteBuffer.parent = null;
            linkedByteBuffer.bubby.parent = null;
            linkedByteBuffer.bubby.bubby = null;
            linkedByteBuffer.bubby = null;
            
            linkedByteBuffer = parent;
        }
        
        linkedByteBuffer.free = true;
        
        freeBuffers[ linkedByteBuffer.level ].offer( linkedByteBuffer );
    }

    @Override
    public List<ByteBuffer> borrow( int size ) throws BufferOverflowException
    {

        checkState( !isClosed() );

        final List<ByteBuffer> byteBuffers = new LinkedList<ByteBuffer>();

        int allocatedSize = 0;
        
        do {
            int sizeToTryToAllocate = Math.min(size - allocatedSize, this.segmentSize);
            
            int level = (int)Math.min( segmentSizeLevel - maxExponantOf2 ( sizeToTryToAllocate ), maxLevel - 1);
            
            if (level < 0) {
            	throw new BufferOverflowException();
            }
            
            LinkedByteBuffer buffer = null;
            
            buffer = freeBuffers[level].poll();
            
            if (buffer == null) {
                
                // find a free buffer
                for (int searchedLevel = level - 1; searchedLevel >= 0; searchedLevel--) {
                    
                    buffer = freeBuffers[searchedLevel].poll();
                    if (buffer != null) {
                        break;
                    }
                }
                
                if (buffer != null) {
                    int newBubbySize;
                    
                    do {
                        buffer.free = false;
                        
                        // split.
                        newBubbySize = buffer.byteBuffer.limit() / 2;
                        
                        buffer.byteBuffer.position( 0 );
                        buffer.byteBuffer.limit(newBubbySize);
                        
                        ByteBuffer b1 = buffer.byteBuffer.slice();
                                            
                        buffer.byteBuffer.position( newBubbySize );
                        buffer.byteBuffer.limit( newBubbySize * 2 );
                        
                        ByteBuffer b2 = buffer.byteBuffer.slice();
    
                        LinkedByteBuffer lb1 = new LinkedByteBuffer( buffer.level + 1, b1 );
                        LinkedByteBuffer lb2 = new LinkedByteBuffer( buffer.level + 1, b2 );
                        
                        lb1.bubby = lb2;
                        lb2.bubby = lb1;
                        lb1.parent = buffer;
                        lb2.parent = buffer;
                        
                        
                        freeBuffers[ lb2.level ].offer( lb2 );
                        
                        buffer = lb1;
                        
                    } while (level > buffer.level);
                }
            }
                
            if (buffer == null) {
            	// free all borrowed buffers
            	for (ByteBuffer alreadyBorrowedBuffer : byteBuffers) {
                    release( alreadyBorrowedBuffer );
                }
                throw new BufferOverflowException();
            }
            
            buffer.free = false;
            
            int bufferLimit = Math.min(buffer.byteBuffer.capacity(), sizeToTryToAllocate);
            
            buffer.byteBuffer.limit(bufferLimit);
            
            borrowedBuffers.put( DirectByteBufferUtils.getHash( buffer.byteBuffer ), buffer );
            
            byteBuffers.add(buffer.byteBuffer);
            
            allocatedSize += buffer.byteBuffer.capacity();
        
        } while (allocatedSize < size);
        
        return byteBuffers;

    }


    @Override
    public void clear()
    {
        for ( final Map.Entry<Integer, LinkedByteBuffer> entry : borrowedBuffers.entrySet() )
        {
            merge( entry.getValue() );
        }
        
        borrowedBuffers.clear();
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
        return minAllocationSize;
    }
    
    /**
     * Bubby representation. Hold link to parent and other bubby.
     * 
     * @author bperroud
     *
     */
    private class LinkedByteBuffer {
        
        private final ByteBuffer byteBuffer;
        private final int level;
        private LinkedByteBuffer bubby;
        private LinkedByteBuffer parent;
        private boolean free = true;
        
        LinkedByteBuffer(int level, ByteBuffer byteBuffer) {
            this.level = level;
            this.byteBuffer = byteBuffer;
        }
    }
    
    /**
     * Queue based on linked nodes. All 3 operations {@link LinkedHashQueue#poll()}, 
     * {@link LinkedHashQueue#offer(Object)} and {@link LinkedHashQueue#remove(Object)} 
     * are done in constant time. 
     * 
     * Note on {@link LinkedHashQueue#remove(Object)}: {@link HashMap} is used internally
     * to achieve constant time for removing.
     * 
     * @param <V> class of the payload
     */
    public static class LinkedHashQueue<V> {
        
        final Entry head = new Entry(null);
        final Map<V, Entry> map = new HashMap<V, Entry>(); // IdentityHashMap
        
        public LinkedHashQueue() {
            head.after = head;
            head.before = head;
        }
        
        public V poll() {
            if (head.after == head) {
                return null;
            }
            
            Entry entry = head.after;
            head.after = entry.after;
            head.after.before = head;
            
            entry.before = null;
            entry.after = null;
            
            map.remove( entry.value );
            return entry.value;
        }
        
        public void offer(V value) {
            
            if (!map.containsKey( value )) {
                final Entry entry = new Entry( value );
                
                final Entry lastEntry = head.before;
                
                lastEntry.after = entry;
                head.before = entry;
                
                entry.before = lastEntry;
                entry.after = head;
                
                map.put( value, entry );
            }
        }
        
        public void remove(V value) {
            final Entry entry = map.remove( value );
            if (entry != null) {
                entry.after.before = entry.before;
                entry.before.after = entry.after;
                entry.before = null;
                entry.after = null;
            }
        }
        
        /**
         * Internal linked node
         */
        class Entry {
            Entry after = null;
            Entry before = null;
            final V value;
            public Entry(V value) {
                this.value = value;
            }
        }
        
    }
    
}
