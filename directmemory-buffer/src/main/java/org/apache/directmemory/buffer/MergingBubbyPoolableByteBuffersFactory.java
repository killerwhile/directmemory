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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.directmemory.memory.allocator.DirectByteBufferUtils;
import org.apache.directmemory.stream.ByteBufferStream;

import static com.google.common.base.Preconditions.checkState;

/**
 * {@link PoolableByteBuffersFactory} implementation that instantiate {@link ByteBuffer}s.
 * Allocation algorithm is based on Knuth's bubby allocation scheme.
 *
 * Initial big segments of off heap memory are split in two bubbies of the same size
 * (half of the parent's size), and then iteratively one bubby is selected and
 * split again till a buffer of the requested size if created and returned to the caller.
 *
 * ... linked nodes ...
 * ... sized optimized version where minExponent is found ...
 *
 * TODO: ensure concurrency correctness.
 * TODO: avoid the need to have a had a bubby bigger or equals to the requested size to be able to allocate.
 *
 * @since 0.2
 */
public class MergingBubbyPoolableByteBuffersFactory
    extends AbstractPoolableByteBuffersFactory
    implements PoolableByteBuffersFactory
{

    private static final int DEFAULT_MIN_ALLOCATION_SIZE = 128;

    // Collection that keeps track of the parent buffers (segments) where slices are allocated
    private final ByteBuffer[] segmentsBuffers;

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

        // Compute the size of each segments. A segment can't be bigger than Integer.MAX_VALUE,
        // so either numberOfSegments is given appropriately, or we force a bigger number of segments.
        int segmentSize = (totalSize / numberOfSegments) > MAX_SEGMENT_SIZE ? (int)MAX_SEGMENT_SIZE : (int)(totalSize / numberOfSegments);

        // segmentSize must be a power of two
        checkState((segmentSize & (segmentSize-1)) == 0 );

        // minAllocationSize must be a power of two
        checkState((minAllocationSize & (minAllocationSize-1)) == 0 );

        numberOfSegments = (int)(totalSize / segmentSize);

        checkState( numberOfSegments > 0 );

        this.minAllocationSize = minAllocationSize;

        this.segmentsBuffers = new ByteBuffer[numberOfSegments];

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
            segmentsBuffers[i] = segment;

            freeBuffers[0].offer( new LinkedByteBuffer( 0, segment ) );

            allocatedSize += segmentSize;
        }

        this.totalSize = allocatedSize;
    }

    /**
     * Returns the smallest e where i <= 2^e
     *
     * @param i
     * @return
     */
    private static int minExponantOf2(int i ) {
        return (int)Math.ceil( Math.log( i ) / Math.log( 2 ) );
    }

    /**
     * Returns the biggest e where i >= 2^e
     *
     * @param i
     * @return
     */
    private static int maxExponantOf2(int i ) {
        return (int)Math.floor( Math.log( i ) / Math.log( 2 ) );
    }

    @Override
    public synchronized void release( final ByteBuffer byteBuffer )
    {

        checkState( !isClosed() );

        LinkedByteBuffer linkedByteBuffer = borrowedBuffers.remove( DirectByteBufferUtils.getHash( byteBuffer ) );

        if ( linkedByteBuffer == null)
        {
            throw new IllegalStateException( "This buffer has already been freeed." );
        }

        if ( linkedByteBuffer.byteBuffer != byteBuffer)
        {
            throw new IllegalStateException( "Hu ? This buffer does not match anything." );
        }

        merge(linkedByteBuffer);
    }


    @Override
    public void release( final List<ByteBuffer> byteBuffers )
    {
        for (final ByteBuffer bb : byteBuffers)
        {
            release(bb);
        }
    }


    private void merge(LinkedByteBuffer linkedByteBuffer) {
        while (linkedByteBuffer.level > 0
                && linkedByteBuffer.bubby != null
                && linkedByteBuffer.bubby.free.compareAndSet(true, false)) {

            freeBuffers[ linkedByteBuffer.level ].remove( linkedByteBuffer.bubby );

            LinkedByteBuffer parent = linkedByteBuffer.parent;
            linkedByteBuffer.parent = null;
            linkedByteBuffer.bubby.parent = null;
            linkedByteBuffer.bubby.bubby = null;
            linkedByteBuffer.bubby = null;

            linkedByteBuffer = parent;

        }

        linkedByteBuffer.free.set(true);

        freeBuffers[ linkedByteBuffer.level ].offer( linkedByteBuffer );

    }

    @Override
    public synchronized List<ByteBuffer> borrow( int size ) throws BufferOverflowException
    {

        checkState( !isClosed() );

        final List<ByteBuffer> byteBuffers = new LinkedList<ByteBuffer>();

        int allocatedSize = 0;

        // Size requested can be bigger than a single segment size.
        // We can thus return multiple ByteBuffers spread across
        // multiple segments.
        do {
            // size to allocate at once can't be bigger than the segmentSize.
            int sizeToTryToAllocate = Math.min(size - allocatedSize, this.segmentSize);

            // compute the level where to start looking at
            int level = (int)Math.min( segmentSizeLevel - maxExponantOf2 ( sizeToTryToAllocate ), maxLevel - 1);

            LinkedByteBuffer buffer = null;

            buffer = freeBuffers[level].poll();

            if (buffer != null && !buffer.free.compareAndSet(true, false)) {
                continue;
            }

            if (buffer == null) {

                boolean freeBufferSplit = false;

                do {
                    // find a free buffer
                    for (int searchedLevel = level - 1; searchedLevel >= 0; searchedLevel--) {

                        buffer = freeBuffers[searchedLevel].poll();
                        if (buffer != null) {

                            // Ensure this buffer is not currently being merged with its bubby
                            if (buffer.free.compareAndSet(true, false)) {
                                break;
                            }
                        }
                    }

                    if (buffer != null) {

                        do {

                            // split, offer one bubby to the free list, keep the second bubby.
                            buffer = splitByteBufferAndOfferOne(buffer);

                        } while (level > buffer.level);

                        freeBufferSplit = true;
                    }

                } while (buffer != null && !freeBufferSplit);
            }

            if (buffer == null) {

                // TODO: Try from a bigger level first before failing directly.

                // free all borrowed buffers
                for (ByteBuffer alreadyBorrowedBuffer : byteBuffers) {
                    release( alreadyBorrowedBuffer );
                }
                throw new BufferOverflowException();
            }

            int bufferLimit = Math.min(buffer.byteBuffer.capacity(), sizeToTryToAllocate);

            // Reset buffer's state
            buffer.byteBuffer.clear();
            buffer.byteBuffer.limit(bufferLimit);

            borrowedBuffers.put( DirectByteBufferUtils.getHash( buffer.byteBuffer ), buffer );

            byteBuffers.add(buffer.byteBuffer);

            allocatedSize += buffer.byteBuffer.limit();

        } while (allocatedSize < size);

        return byteBuffers;

    }


    private LinkedByteBuffer splitByteBufferAndOfferOne(LinkedByteBuffer parentByteBuffer) {


        int newBubbySize = parentByteBuffer.byteBuffer.limit() / 2;

        parentByteBuffer.byteBuffer.position( 0 );
        parentByteBuffer.byteBuffer.limit(newBubbySize);

        ByteBuffer b1 = parentByteBuffer.byteBuffer.slice();

        parentByteBuffer.byteBuffer.position( newBubbySize );
        parentByteBuffer.byteBuffer.limit( newBubbySize * 2 );

        ByteBuffer b2 = parentByteBuffer.byteBuffer.slice();

        // lb1 will be returned or split again, it must be marked as in use.
        LinkedByteBuffer lb1 = new LinkedByteBuffer( parentByteBuffer.level + 1, b1, false );

        // lb2 is offered to the pool of free buffers.
        LinkedByteBuffer lb2 = new LinkedByteBuffer( parentByteBuffer.level + 1, b2 );

        lb1.bubby = lb2;
        lb2.bubby = lb1;
        lb1.parent = parentByteBuffer;
        lb2.parent = parentByteBuffer;

        freeBuffers[ lb2.level ].offer( lb2 );

        return lb1;
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
        private final AtomicBoolean free;

        LinkedByteBuffer(int level, ByteBuffer byteBuffer, boolean free) {
            this.level = level;
            this.byteBuffer = byteBuffer;
            this.free = new AtomicBoolean(free);
        }

        LinkedByteBuffer(int level, ByteBuffer byteBuffer) {
            this(level, byteBuffer, true);
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

        public synchronized V poll() {
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

        public synchronized void offer(V value) {

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

        public synchronized void remove(V value) {
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
