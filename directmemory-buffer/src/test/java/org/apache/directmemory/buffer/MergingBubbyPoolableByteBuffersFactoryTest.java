package org.apache.directmemory.buffer;


import junit.framework.Assert;

import org.apache.directmemory.buffer.MergingBubbyPoolableByteBuffersFactory.LinkedHashQueue;
import org.apache.directmemory.measures.Ram;
import org.junit.Test;

public class MergingBubbyPoolableByteBuffersFactoryTest extends AbstractPoolableByteBuffersFactoryTest
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


    @Test(expected = IllegalStateException.class)
    public void testInstanciateWithOddNumbers() {

        final int SLICE_SIZE = 100;
        final int TOTAL_SIZE = Ram.Mb( 2 ) - 1;

        getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, 1 );

    }

    @Test(expected = IllegalStateException.class)
    public void testBorrowAndReleaseWithOddNumbers() {

        final int SLICE_SIZE = 100;
        final int TOTAL_SIZE = Ram.Mb( 2 ) - 1;

        getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, 1 );

    }

    @Override
    protected PoolableByteBuffersFactory getPoolableByteBuffersFactory(
            long totalSize, int bufferSize, int numberOfSegments) {
        return new MergingBubbyPoolableByteBuffersFactory( totalSize, numberOfSegments, bufferSize );
    }
}
