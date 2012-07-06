package org.apache.directmemory.buffer;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.apache.directmemory.buffer.MergingBubbyPoolableByteBuffersFactory.LinkedHashQueue;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.stream.ByteBufferInputStream;
import org.apache.directmemory.stream.ByteBufferOutputStream;
import org.apache.directmemory.stream.ByteBufferStream;
import org.junit.Test;

public class MergingBubbyPoolableByteBuffersFactoryTest
{

    
    @Test
    public void testLinkeHashQueue() {
        
        LinkedHashQueue<Integer> lhq = new LinkedHashQueue<Integer>();
        
        lhq.offer( Integer.valueOf(1) );

        lhq.offer( Integer.valueOf(3) );

        lhq.offer( Integer.valueOf(2) );

        lhq.offer( Integer.valueOf(6) );

        lhq.offer( Integer.valueOf(4) );
        lhq.offer( Integer.valueOf(1) );

        Assert.assertEquals(Integer.valueOf(1), lhq.poll());
        Assert.assertEquals(Integer.valueOf(3), lhq.poll());
        
        lhq.remove( Integer.valueOf(6) );
        
        Assert.assertEquals(Integer.valueOf(2), lhq.poll());
        Assert.assertEquals(Integer.valueOf(4), lhq.poll());
        
        Assert.assertNull(lhq.poll());

        lhq.offer( Integer.valueOf(5) );

        Assert.assertEquals(Integer.valueOf(5), lhq.poll());

        Assert.assertNull(lhq.poll());

    }
    
    
    @Test
    public void testBorrowAndReleaseSequentially() {
        
        final int SLICE_SIZE = 128;
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = new MergingBubbyPoolableByteBuffersFactory( TOTAL_SIZE, 1 );
        
        borrowAndRelease( factory, SLICE_SIZE, 1, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE, 2, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE - SLICE_SIZE / 2, 2, SLICE_SIZE, SLICE_SIZE / 2 );
        
        borrowAndRelease( factory, TOTAL_SIZE, 1, TOTAL_SIZE, TOTAL_SIZE );
        
        borrowAndRelease( factory, SLICE_SIZE, 1, SLICE_SIZE, SLICE_SIZE );
        
    }
    
    
    @Test(expected = BufferOverflowException.class)
    public void testAllocateTooBigSize() {
        
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = new MergingBubbyPoolableByteBuffersFactory( TOTAL_SIZE, 1 );

        factory.borrow( TOTAL_SIZE + 1 );
        
    }
    
    
    @Test
    public void testRecoverAfterTryingToAllocateTooBigSize() {
        
        final int SLICE_SIZE = 128;
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = new MergingBubbyPoolableByteBuffersFactory( TOTAL_SIZE, 1 );
        
        borrowAndRelease( factory, SLICE_SIZE, 1, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE, 2, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE - SLICE_SIZE / 2, 2, SLICE_SIZE, SLICE_SIZE / 2 );
        
        borrowAndRelease( factory, TOTAL_SIZE, 1, TOTAL_SIZE, TOTAL_SIZE );
        
        borrowAndRelease( factory, SLICE_SIZE, 1, SLICE_SIZE, SLICE_SIZE );

        try {
            factory.borrow( TOTAL_SIZE + 1 );
            Assert.fail( "BufferOverflowException should have been thrown" );
        } catch (BufferOverflowException e) {
            // It's ok.
        }
        
        borrowAndRelease( factory, SLICE_SIZE, 1, SLICE_SIZE, SLICE_SIZE );
        
    }
    
    @Test
    public void testNPlus1BorrowAndRelease() {
        
        final int SLICE_SIZE = 128;
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = new MergingBubbyPoolableByteBuffersFactory( TOTAL_SIZE, 1 );
        
        for (int i = 0; i < (int)Math.ceil( TOTAL_SIZE / SLICE_SIZE ) + 1; i++) {
            borrowAndRelease( factory, SLICE_SIZE, 1, SLICE_SIZE, SLICE_SIZE );
        }
        
    }
    
    
    @Test(expected = IllegalStateException.class)
    public void testReleaseBuffersTwice() {
        
        final int SLICE_SIZE = 128;
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = new MergingBubbyPoolableByteBuffersFactory( TOTAL_SIZE, 1 );

        List<ByteBuffer> buffers = factory.borrow( 2 * SLICE_SIZE );
        
        release( factory, buffers );
        
        release( factory, buffers );
        
    }
    
    
    @Test
    public void testBorrowAndReleaseSequentiallyWithSeveralSegments() {
        
        final int SLICE_SIZE = 128;
        final int TOTAL_SIZE = Ram.Mb( 2 );
        
        final PoolableByteBuffersFactory factory = new MergingBubbyPoolableByteBuffersFactory( TOTAL_SIZE, 2 );
        
        borrowAndRelease( factory, SLICE_SIZE, 1, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE, 2, SLICE_SIZE, SLICE_SIZE );
        
        borrowAndRelease( factory, 3 * SLICE_SIZE - SLICE_SIZE / 2, 2, SLICE_SIZE, SLICE_SIZE / 2 );
        
        borrowAndRelease( factory, TOTAL_SIZE, 2, TOTAL_SIZE / 2, TOTAL_SIZE / 2 );
        
        borrowAndRelease( factory, SLICE_SIZE, 1, SLICE_SIZE, SLICE_SIZE );
        
    }
    
    
    @Test
    public void testStream() throws IOException {
        
        final int SLICE_SIZE = 32;
        final int TOTAL_SIZE = 512;
        final int NUMBER_OF_SEGMENTS = 4;
        final int BYTES_WRITTEN = 500;
        
        final PoolableByteBuffersFactory factory = new MergingBubbyPoolableByteBuffersFactory( TOTAL_SIZE, NUMBER_OF_SEGMENTS, SLICE_SIZE );
        
        ByteBufferStream stream = factory.getInOutStream();
        
        ByteBufferOutputStream out = stream.getOutputStream();
        final Random r = new Random( System.nanoTime() );
        final byte[] bytesWritten = new byte[BYTES_WRITTEN];
        
        for (int i = 0; i < BYTES_WRITTEN; i++) {
            bytesWritten[i] = (byte)r.nextInt( 0xff );
            out.write( bytesWritten[i] );
        }
        
        ByteBufferInputStream in1 = stream.getInputStream();
        final byte[] bytesRead1 = new byte[BYTES_WRITTEN];
        
        ByteBufferInputStream in2 = stream.getInputStream();
        final byte[] bytesRead2 = new byte[BYTES_WRITTEN];
        
        for (int i = 0; i < BYTES_WRITTEN; i++) {
            bytesRead1[i] = (byte)in1.read();
            bytesRead2[i] = (byte)in2.read();
        }
        
        for (int i = 0; i < BYTES_WRITTEN; i++) {
            Assert.assertEquals( "Error 1 at index " + i + " of " + bytesWritten.length, bytesWritten[i], bytesRead1[i] );
            Assert.assertEquals( "Error 2 at index " + i + " of " + bytesWritten.length, bytesWritten[i], bytesRead2[i] );
        }
        
    }
    
    

    @Test
    public void testStream2() throws IOException {
        
        final int SLICE_SIZE = 32;
        final int TOTAL_SIZE = 512;
        final int NUMBER_OF_SEGMENTS = 4;
        
        final PoolableByteBuffersFactory factory = new MergingBubbyPoolableByteBuffersFactory( TOTAL_SIZE, NUMBER_OF_SEGMENTS, SLICE_SIZE );
        
        ByteBufferStream stream = factory.getInOutStream();
        
        ByteBufferOutputStream out = stream.getOutputStream();
        final Random r = new Random( System.nanoTime() );
        final byte[] bytesWritten = new byte[r.nextInt( TOTAL_SIZE )];
        
        r.nextBytes( bytesWritten );
        
        out.write( bytesWritten );

        ByteBufferInputStream in1 = stream.getInputStream();
        final byte[] bytesRead1 = new byte[bytesWritten.length];
        
        ByteBufferInputStream in2 = stream.getInputStream();
        final byte[] bytesRead2 = new byte[bytesWritten.length];
        
        in1.read(bytesRead1);
        in2.read(bytesRead2);
        
        for (int i = 0; i < bytesWritten.length; i++) {
            Assert.assertEquals( "Error 1 at index " + i + " of " + bytesWritten.length, bytesWritten[i], bytesRead1[i] );
            Assert.assertEquals( "Error 2 at index " + i + " of " + bytesWritten.length, bytesWritten[i], bytesRead2[i] );
        }
        
    }
    
    @Test
    public void testStream3() throws IOException {
        
        final int SLICE_SIZE = 32;
        final int TOTAL_SIZE = 512;
        final int NUMBER_OF_SEGMENTS = 4;
        
        final PoolableByteBuffersFactory factory = new MergingBubbyPoolableByteBuffersFactory( TOTAL_SIZE, NUMBER_OF_SEGMENTS, SLICE_SIZE );
        
        ByteBufferStream stream = factory.getInOutStream();
        
        ByteBufferOutputStream out = stream.getOutputStream();
        final Random r = new Random( System.nanoTime() );
        final byte[] bytesWritten = new byte[r.nextInt( TOTAL_SIZE )];
        
        r.nextBytes( bytesWritten );
        
        int b = 0;
        while (b < bytesWritten.length) {
            int w = r.nextInt(bytesWritten.length - b) + 1;
            out.write( bytesWritten, b, w );
            b += w;
        }
        

        ByteBufferInputStream in = stream.getInputStream();
        final byte[] bytesRead = new byte[bytesWritten.length];
        
        int a = 0;
        b = 0;
        while ((a = in.available()) > 0) {
            int l = r.nextInt( a ) + 1;
            int m = in.read( bytesRead, b, l );
            b += m;
        }
            
        for (int i = 0; i < bytesWritten.length; i++) {
            Assert.assertEquals( "Error 1 at index " + i + " of " + bytesWritten.length, bytesWritten[i], bytesRead[i] );
        }
        
    }
    
    @Test
    public void testStream4() throws IOException {
        
        for (int c = 0; c < 10; c++) {
        final int SLICE_SIZE = 32;
        final int TOTAL_SIZE = 512;
        final int NUMBER_OF_SEGMENTS = 4;
        
        final PoolableByteBuffersFactory factory = new MergingBubbyPoolableByteBuffersFactory( TOTAL_SIZE, NUMBER_OF_SEGMENTS, SLICE_SIZE );
        
        ByteBufferStream stream = factory.getInOutStream();
        
        ByteBufferOutputStream out = stream.getOutputStream();
        final Random r = new Random( System.nanoTime() );
        final byte[] bytesWritten = new byte[TOTAL_SIZE];
        
        r.nextBytes( bytesWritten );

        try{
        int b = 0;
        while (b < bytesWritten.length) {
            int w = r.nextInt(bytesWritten.length - b) + 1;
            out.write( bytesWritten, b, w );
            b += w;
        }
        
        ByteBufferInputStream in = stream.getInputStream();
        final byte[] bytesRead = new byte[bytesWritten.length];
        
        int a = 0;
        b = 0;
        while ((a = in.available()) > 0) {
            int l = r.nextInt( a ) + 1;
            int m = in.read( bytesRead, b, l );
            b += m;
        }
            
        for (int i = 0; i < bytesWritten.length; i++) {
            Assert.assertEquals( "Error 1 at index " + i, bytesWritten[i], bytesRead[i] );
        }
        }catch (BufferOverflowException e){
            e.printStackTrace();
        }
        }
    }
    
    @Test
    public void testStream5() throws IOException {
        
        final int SLICE_SIZE = 32;
        final int TOTAL_SIZE = 512;
        final int NUMBER_OF_SEGMENTS = 4;
        
        final PoolableByteBuffersFactory factory = new MergingBubbyPoolableByteBuffersFactory( TOTAL_SIZE, NUMBER_OF_SEGMENTS, SLICE_SIZE );
        
        ByteBufferStream stream = factory.getInOutStream();
        
        ByteBufferOutputStream out = stream.getOutputStream();
        final Random r = new Random( System.nanoTime() );
        final byte[] bytesWritten1 = new byte[ r.nextInt(TOTAL_SIZE / 2)];
        
        r.nextBytes( bytesWritten1 );

        int b1 = 0;
        while (b1 < bytesWritten1.length) {
            int w1 = r.nextInt(bytesWritten1.length - b1) + 1;
            out.write( bytesWritten1, b1, w1 );
            b1 += w1;
        }
        
        ByteBufferInputStream in = stream.getInputStream();
        final byte[] bytesRead1 = new byte[bytesWritten1.length];
        
        int a1 = 0;
        b1 = 0;
        while ((a1 = in.available()) > 0) {
            int l = r.nextInt( a1 ) + 1;
            int m = in.read( bytesRead1, b1, l );
            b1 += m;
        }
        
        final byte[] bytesWritten2 = new byte[ r.nextInt(TOTAL_SIZE / 2)];
        
        r.nextBytes( bytesWritten2 );

        int b2 = 0;
        while (b2 < bytesWritten2.length) {
            int w2 = r.nextInt(bytesWritten2.length - b2) + 1;
            out.write( bytesWritten2, b2, w2 );
            b2 += w2;
        }
        
        final byte[] bytesRead2 = new byte[bytesWritten2.length];
        
        int a2 = 0;
        b2 = 0;
        while ((a2 = in.available()) > 0) {
            int l = r.nextInt( a2 ) + 1;
            int m = in.read( bytesRead2, b2, l );
            b2 += m;
        }
        
        
        for (int i = 0; i < bytesWritten1.length; i++) {
            Assert.assertEquals( "Error 1 at index " + i + " of " + bytesWritten1.length, bytesWritten1[i], bytesRead1[i] );
        }
        
        for (int i = 0; i < bytesWritten2.length; i++) {
            Assert.assertEquals( "Error 2 at index " + i + " of " + bytesWritten2.length, bytesWritten2[i], bytesRead2[i] );
        }
        
    }
    
    
    
    private void borrowAndRelease(PoolableByteBuffersFactory factory, int requestedSliceSize, int expectedNumberOfSlices, int expectedLastSliceCapacity, int expectedLastBufferUsableSize) {
        
        List<ByteBuffer> buffers = factory.borrow( requestedSliceSize );
        
        Assert.assertEquals( expectedNumberOfSlices, buffers.size() );
        Assert.assertEquals( expectedLastSliceCapacity, buffers.get(expectedNumberOfSlices - 1).capacity() );
        Assert.assertEquals( expectedLastBufferUsableSize, buffers.get(expectedNumberOfSlices - 1).limit() );
        
        int sum = 0;
        for (ByteBuffer bb : buffers) {
            sum += bb.limit();
        }
        
        Assert.assertEquals( requestedSliceSize, sum );
        
        release( factory, buffers );
    }
    
    private void release(PoolableByteBuffersFactory factory, List<ByteBuffer> buffers) {
        for (ByteBuffer buffer : buffers) {
            factory.release( buffer );
        }
    }
}
