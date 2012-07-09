package org.apache.directmemory.buffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.directmemory.measures.Ram;
import org.junit.Ignore;
import org.junit.Test;

public class FixedSizePoolableByteBuffersFactoryTest extends AbstractPoolableByteBuffersFactoryTest
{



    @Test
    public void testBinarySearch() {

        List<Integer> indexes = new ArrayList<Integer>();
        indexes.add(1);
        indexes.add(4);
        indexes.add(10);
        indexes.add(18);
        indexes.add(33);
        indexes.add(99);
        indexes.add(1089);

        Assert.assertEquals( 0, Collections.binarySearch( indexes, Integer.valueOf(1) ) );
        Assert.assertEquals( 1, Collections.binarySearch( indexes, Integer.valueOf(4) ) );
        Assert.assertEquals( 2, Collections.binarySearch( indexes, Integer.valueOf(10) ) );
        Assert.assertEquals( -1, Collections.binarySearch( indexes, Integer.valueOf(0) ) );
        Assert.assertEquals( -6, Collections.binarySearch( indexes, Integer.valueOf(50) ) );
        Assert.assertEquals( -8, Collections.binarySearch( indexes, Integer.valueOf(2000) ) );

    }

    @Test
    public void testBorrowAndReleaseWithOddNumbers() {

        final int SLICE_SIZE = 100;
        final int TOTAL_SIZE = Ram.Mb( 2 ) - 1;

        final PoolableByteBuffersFactory factory = getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, 1 );

        borrowAndRelease( factory, SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );

        borrowAndRelease( factory, 3 * SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );

        borrowAndRelease( factory, 3 * SLICE_SIZE - SLICE_SIZE / 2, SLICE_SIZE, SLICE_SIZE / 2 );

        int numberOfBuffers = (int)Math.floor( TOTAL_SIZE / SLICE_SIZE );
        borrowAndRelease( factory, numberOfBuffers * SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );

        borrowAndRelease( factory, SLICE_SIZE,  SLICE_SIZE, SLICE_SIZE );

    }


    @Test
    public void testInstanciateWithOddNumbers() {

        final int SLICE_SIZE = 99;
        final int TOTAL_SIZE = Ram.Mb( 2 ) + 1;
        final int NUMBER_OF_SEGMENTS = 3;

        final PoolableByteBuffersFactory factory = getPoolableByteBuffersFactory( TOTAL_SIZE, SLICE_SIZE, NUMBER_OF_SEGMENTS );

        borrowAndRelease( factory, SLICE_SIZE, SLICE_SIZE, SLICE_SIZE );

    }



    @Test
    @Ignore
    public void testHashCode() {

        int backupI = 0;

        try {
            Map<Integer, Integer> map = new HashMap<Integer, Integer>();

            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                backupI = i;
                Integer h = Integer.valueOf( hash(i) );
                Integer value = map.get( h );

                Assert.assertTrue( "Collision found: " + h + " = hash( " + i + " ) = hash ( " + value + " ) !!", value == null );

                map.put( h, Integer.valueOf( i ) );
            }

        } finally {
            System.out.println("BackupI = " + backupI);
        }
    }

    private static int hash(int i) {
        return ((i << 7) - i + (i >>> 9) + (i >>> 17));
    }



    @Override
    protected PoolableByteBuffersFactory getPoolableByteBuffersFactory(long totalSize, int bufferSize, int numberOfSegments) {
        return new FixedSizePoolableByteBuffersFactory( totalSize, bufferSize, numberOfSegments );
    }
}
