package org.apache.directmemory.stream;

import org.apache.directmemory.buffer.FixedSizePoolableByteBuffersFactory;
import org.apache.directmemory.buffer.PoolableByteBuffersFactory;

public class MergingBubbyByteBufferStreamTest extends AbstractByteBufferStreamTest{

    @Override
    protected PoolableByteBuffersFactory getPoolableByteBuffersFactory(long totalSize, int bufferSize, int numberOfSegments) {
        return new FixedSizePoolableByteBuffersFactory( totalSize, bufferSize, numberOfSegments );
    }

}
