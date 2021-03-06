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

import java.io.Closeable;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.directmemory.stream.ByteBufferStream;

/**
 * Lean {@link ByteBuffer}s Factory interface.
 *
 * @since 0.2
 */
public interface PoolableByteBuffersFactory extends Closeable
{

    /**
     * Allocates and returns a {@link List} of {@link ByteBuffer} with all {@link ByteBuffer#limit()} set to a given size.
     * When the allocation fails, it throws an {@link BufferOverflowException}.
     * @param size : the size in byte to allocate
     * @return a {@link List} of {@link ByteBuffer} with the total capacity of the requested size, or throws an {@link BufferOverflowException} if the allocation fails.
     * @throws BufferOverflowException when not enough available space available.
     */
    List<ByteBuffer> borrow( final int size ) throws BufferOverflowException;

    /**
     * Get a {@link ByteBufferStream}, which enable streaming in and out {@link ByteBuffer} transparantly, without giving initial size.
     *
     * @return {@link ByteBufferStream} with the current {@link PoolableByteBuffersFactory} as the factory.
     */
    ByteBufferStream getInOutStream();

    /**
     * Returns the given {@link ByteBuffer} to the factory, available it for a future usage.
     * Returning twice a {@link ByteBuffer} will throw an {@link IllegalStateException}.
     * @param buffer : the {@link ByteBuffer} to return
     */
    void release( final ByteBuffer buffer );

    /**
     * Returns the given {@link List} of {@link ByteBuffer}s to the factory, available it for a future usage.
     * Returning twice the same {@link ByteBuffer} will throw an {@link IllegalStateException}.
     * @param buffer : the {@link ByteBuffer} to return
     */
    void release( final List<ByteBuffer> buffers );


    /**
     * Clear all allocated {@link ByteBuffer}, resulting in a empty and ready to deserve {@link PoolableByteBuffersFactory}
     */
    void clear();

    /**
     * @return the total size that can be allocated
     */
    long getCapacity();

    /**
     * @return the advised size to be allocated. Allocating more than this size is allowed, the result is implementation dependent.
     *   Allocating a multiple of this size is always a good idea.
     */
    int getDefaultAllocationSize();

}
