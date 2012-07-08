package org.apache.directmemory.buffer;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.allocator.DirectByteBufferUtils;
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
    
    
    
    @Test
    public void testMT() throws InterruptedException {
        
        final int SLICE_SIZE = 512;
        final int TOTAL_SIZE = Ram.Mb( 4 );
        final int NUM_THREADS = 4;
        
        final ExecutorService e = Executors.newFixedThreadPool(NUM_THREADS);
        
        final PoolableByteBuffersFactory factory = getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, NUM_THREADS );
        final CountDownLatch latch = new CountDownLatch(1);
        
        final AtomicBoolean failure = new AtomicBoolean(false);
        
        for (int i = 0; i < NUM_THREADS; i++) {
        	e.execute(new MTRun1(factory, latch, TOTAL_SIZE / NUM_THREADS, failure));
        }
        
        latch.countDown();
        
        e.shutdown();
        
        e.awaitTermination(10, TimeUnit.SECONDS);
        
        Assert.assertFalse(failure.get());
        
    }

    private static class MTRun1 implements Runnable {

    	final PoolableByteBuffersFactory factory;
    	final CountDownLatch latch;
    	final int maxSize;
    	final AtomicBoolean failure;
    	
    	final Random r = new Random();
    	
    	public MTRun1(final PoolableByteBuffersFactory factory, final CountDownLatch latch, int maxSize, final AtomicBoolean failure) {
    		this.factory = factory;
    		this.latch = latch;
    		this.maxSize = maxSize;
    		this.failure = failure;
    	}
    	
		@Override
		public void run() {
			
			try {
				
				// synchronize the run to achieve more concurrency
				latch.await();
				
				for (int n = 0; n < 10; n++) {
					byte[] b = new byte[r.nextInt(maxSize)];
					r.nextBytes(b);
					
					List<ByteBuffer> bbs = factory.borrow(b.length);

//					for (ByteBuffer bb : bbs) {
//						System.out.println("borrow " + DirectByteBufferUtils.getHash(bb));
//					}
					
					write(bbs, b);
					
					byte[] read = read(bbs);
					
					Assert.assertEquals(b.length, read.length);
					
					for (int i = 0; i < b.length; i++) {
						if (b[i] != read[i])
						{
							System.out.println("error at index " + i + " in byteBuffer");
							failure.set(true);
							break;
						}
					}
					
					for (ByteBuffer bb : bbs) {
						factory.release(bb);
//						System.out.println("release " + DirectByteBufferUtils.getHash(bb));
					}
				}
				
			} catch (InterruptedException e) {
				Assert.fail(e.getMessage());
			}
		}
		
		private static void write(List<ByteBuffer> bbs, byte[] b) {
			int w = 0;
			for (ByteBuffer bb : bbs) {
				Assert.assertEquals(0, bb.position());
				int l = (int)Math.min(bb.limit(), b.length - w);
				bb.put(b, w, l);
				w+=l;
			}
			Assert.assertEquals(b.length, w);
		}
		
		private static byte[] read(List<ByteBuffer> bbs) {
			
			int i = 0;
			for (ByteBuffer bb : bbs) {
				i+= bb.limit();
			}
			final byte[] b = new byte[i];
			
			int r = 0;
			for (ByteBuffer bb : bbs) {
				bb.rewind();
				int l = (int)Math.min(bb.limit(), b.length - r);
				bb.get(b, r, l);
				r += l;
			}
			return b;
		}
    	
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
