package org.apache.directmemory.buffer;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.directmemory.measures.Ram;
import org.junit.Ignore;
import org.junit.Test;

import junit.framework.Assert;

public abstract class AbstractPoolableByteBuffersFactoryTest {

	protected abstract PoolableByteBuffersFactory getPoolableByteBuffersFactory(long totalSize, int bufferSize, int numberOfSegments);
	
	
	@Test
    public void testBorrowAndReleaseSequentially() {
        
        final int SLICE_SIZE = 128;
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, 1 );
        
        borrowAndRelease( factory, SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE - SLICE_SIZE / 2, SLICE_SIZE, SLICE_SIZE / 2 );
        
        borrowAndRelease( factory, TOTAL_SIZE );
        
        borrowAndRelease( factory, SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );
        
    }
    
    
    @Test(expected = BufferOverflowException.class)
    public void testAllocateTooBigSize() {
        
        final int SLICE_SIZE = 128;
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, 1 );

        factory.borrow( TOTAL_SIZE + 1 );
        
    }
    
    
    @Test
    public void testRecoverAfterTryingToAllocateTooBigSize() {
        
        final int SLICE_SIZE = 128;
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, 1 );
        
        borrowAndRelease( factory, SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE - SLICE_SIZE / 2, SLICE_SIZE, SLICE_SIZE / 2 );
        
        borrowAndRelease( factory, TOTAL_SIZE );
        
        borrowAndRelease( factory, SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );

        try {
            factory.borrow( TOTAL_SIZE + 1 );
            Assert.fail( "BufferOverflowException should have been thrown" );
        } catch (BufferOverflowException e) {
            // It's ok.
        }
        
        borrowAndRelease( factory, SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );
        
    }
    
    @Test
    public void testNPlus1BorrowAndRelease() {
        
        final int SLICE_SIZE = 128;
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, 1 );
        
        for (int i = 0; i < (int)Math.ceil( TOTAL_SIZE / SLICE_SIZE ) + 1; i++) {
            borrowAndRelease( factory, SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );
        }
        
    }
    
    @Test(expected = IllegalStateException.class)
    public void testReleaseBuffersTwice() {
        
        final int SLICE_SIZE = 128;
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, 1 );

        List<ByteBuffer> buffers = factory.borrow( 2 * SLICE_SIZE );
        
        release( factory, buffers );
        
        release( factory, buffers );
        
    }
    
    
    @Test
    public void testBorrowAndReleaseSequentiallyWithSeveralSegments() {
        
        final int SLICE_SIZE = 128;
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, TOTAL_SIZE / SLICE_SIZE / 2 );
        
        borrowAndRelease( factory, SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE - SLICE_SIZE / 2,  SLICE_SIZE, SLICE_SIZE / 2 );
        
        borrowAndRelease( factory, TOTAL_SIZE );
        
        borrowAndRelease( factory, SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );
        
    }
    
    /**
     * Set the following JVM parameter to be able to run this test
     *   -XX:MaxDirectMemorySize=5G
     * 
     */
    @Test
    @Ignore
    public void testBorrowMoreThan4GB() {
        
        final int SLICE_SIZE = Ram.Mb( 4 );
        final long TOTAL_SIZE = 0L + SLICE_SIZE * 2 + Integer.MAX_VALUE;
        
        final PoolableByteBuffersFactory factory = getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, 1 );
        
        borrowAndRelease( factory, SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );
        
    }


    protected void borrowAndRelease(PoolableByteBuffersFactory factory, int requestedSliceSize, int expectedLastSliceCapacity, int expectedLastBufferUsableSize) {
        
        List<ByteBuffer> buffers = factory.borrow( requestedSliceSize );
        
        Assert.assertEquals( expectedLastSliceCapacity, buffers.get(buffers.size() - 1).capacity() );
        Assert.assertEquals( expectedLastBufferUsableSize, buffers.get(buffers.size() - 1).limit() );
        
        int sum = 0;
        for (ByteBuffer bb : buffers) {
            sum += bb.limit();
        }
        
        Assert.assertEquals( requestedSliceSize, sum );
        
        release( factory, buffers );
    }

	protected void borrowAndRelease(PoolableByteBuffersFactory factory, int requestedSliceSize) {
    
    List<ByteBuffer> buffers = factory.borrow( requestedSliceSize );
    
    int sum = 0;
    for (ByteBuffer bb : buffers) {
        sum += bb.limit();
    }
    
    Assert.assertEquals( requestedSliceSize, sum );
    
    release( factory, buffers );
}
    
    protected void release(PoolableByteBuffersFactory factory, List<ByteBuffer> buffers) {
        for (ByteBuffer buffer : buffers) {
            factory.release( buffer );
        }
    }
    
}
