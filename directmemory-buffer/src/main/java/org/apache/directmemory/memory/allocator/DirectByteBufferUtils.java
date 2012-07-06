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
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Utility class around direct {@link ByteBuffer} 
 *
 */
public class DirectByteBufferUtils
{
    public static final boolean CLEAN_SUPPORTED;
    private static final Method directBufferCleaner;
    private static final Method directBufferCleanerClean;

    static {
        Method directBufferCleanerX = null;
        Method directBufferCleanerCleanX = null;
        boolean supported;
        try {
            directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
            directBufferCleanerX.setAccessible(true);
            directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
            directBufferCleanerCleanX.setAccessible(true);
            supported = true;
        } catch (Exception e) {
            supported = false;
        }
        CLEAN_SUPPORTED = supported;
        directBufferCleaner = directBufferCleanerX;
        directBufferCleanerClean = directBufferCleanerCleanX;
    }
    
    /**
     * DirectByteBuffers are garbage collected by using a phantom reference and
     * a reference queue. Every once a while, the JVM checks the reference queue
     * and cleans the DirectByteBuffers. However, as this doesn't happen
     * immediately after discarding all references to a DirectByteBuffer, it's
     * easy to OutOfMemoryError yourself using DirectByteBuffers. This function
     * explicitly calls the Cleaner method of a DirectByteBuffer.
     * 
     * @param toBeDestroyed
     *            The {@link ByteBuffer} that will be "cleaned". Utilizes
     *            reflection.
     * 
     */
    public static void destroyDirectByteBuffer( final ByteBuffer buffer )
    {

        checkArgument( buffer.isDirect(), "ByteBuffer to destroy must be direct" );

        if ( CLEAN_SUPPORTED )
        {
            try
            {
                Object cleaner = directBufferCleaner.invoke( buffer );
                directBufferCleanerClean.invoke( cleaner );
            }
            catch ( Exception e )
            {
                // silently ignore exception
            }
        }

    }
    
    
    public static Integer getHash( final ByteBuffer buffer )
    {
        final int hashCode = System.identityHashCode( buffer );
//      return ((hashCode << 7) - hashCode + (hashCode >>> 9) + (hashCode >>> 17));
        return hashCode;
    }
    
}
