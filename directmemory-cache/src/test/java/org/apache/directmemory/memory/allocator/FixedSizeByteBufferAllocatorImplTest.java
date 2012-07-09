package org.apache.directmemory.memory.allocator;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.junit.Test;

public class FixedSizeByteBufferAllocatorImplTest
{
    @Test
    public void allocationTest()
    {

        ByteBufferAllocator allocator = new FixedSizeByteBufferAllocatorImpl( 0, 5000, 256, 1 );

        ByteBuffer bf1 = allocator.allocate( 250 );
        Assert.assertEquals( 256, bf1.capacity() );
        Assert.assertEquals( 250, bf1.limit() );

        ByteBuffer bf2 = allocator.allocate( 251 );
        Assert.assertEquals( 256, bf2.capacity() );
        Assert.assertEquals( 251, bf2.limit() );

        ByteBuffer bf3 = allocator.allocate( 200 );
        Assert.assertEquals( 256, bf3.capacity() );
        Assert.assertEquals( 200, bf3.limit() );

        ByteBuffer bf4 = allocator.allocate( 2000 );
        Assert.assertNull( bf4 );

        ByteBuffer bf5 = allocator.allocate( 298 );
        Assert.assertNull( bf5 );

        ByteBuffer bf6 = allocator.allocate( 128 );
        Assert.assertEquals( 256, bf6.capacity() );
        Assert.assertEquals( 128, bf6.limit() );

    }


    @Test
    public void releaseTest()
    {

        ByteBufferAllocator allocator = new FixedSizeByteBufferAllocatorImpl( 0, 1000, 256, 1 );

        ByteBuffer bf1 = allocator.allocate( 250 );
        Assert.assertEquals( 256, bf1.capacity() );
        Assert.assertEquals( 250, bf1.limit() );

        ByteBuffer bf2 = allocator.allocate( 251 );
        Assert.assertEquals( 256, bf2.capacity() );
        Assert.assertEquals( 251, bf2.limit() );

        ByteBuffer bf3 = allocator.allocate( 252 );
        Assert.assertEquals( 256, bf3.capacity() );
        Assert.assertEquals( 252, bf3.limit() );

        ByteBuffer bf4 = allocator.allocate( 500 );
        Assert.assertNull( bf4 );

        allocator.free( bf1 );
        allocator.free( bf2 );

        ByteBuffer bf5 = allocator.allocate( 500 );
        Assert.assertNull( bf5 );

        ByteBuffer bf6 = allocator.allocate( 249 );
        Assert.assertEquals( 256, bf6.capacity() );
        Assert.assertEquals( 249, bf6.limit() );

        ByteBuffer bf7 = allocator.allocate( 248 );
        Assert.assertEquals( 256, bf7.capacity() );
        Assert.assertEquals( 248, bf7.limit() );

    }

    @Test
    public void allocateAndFreeTest()
    {

        ByteBufferAllocator allocator = new FixedSizeByteBufferAllocatorImpl( 0, 1000, 256, 1 );

        for (int i = 0; i < 1000; i++)
        {
            ByteBuffer bf1 = allocator.allocate( 250 );
            Assert.assertEquals( 256, bf1.capacity() );
            Assert.assertEquals( 250, bf1.limit() );

            allocator.free( bf1 );
        }


        ByteBuffer bf2 = allocator.allocate( 1000 );
        Assert.assertNull( bf2 );

        for (int i = 0; i < 3; i++)
        {
            ByteBuffer bf3 = allocator.allocate( 250 );
            Assert.assertEquals( 256, bf3.capacity() );
            Assert.assertEquals( 250, bf3.limit() );

        }

        ByteBuffer bf4 = allocator.allocate( 238 );
        Assert.assertNull( bf4 );

    }

}
