package org.apache.directmemory.guice;

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

import java.util.concurrent.ConcurrentMap;

import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.cache.CacheServiceImpl;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;

import com.google.inject.Inject;
import com.google.inject.Provider;

public final class CacheServiceProvider<K, V>
    implements Provider<CacheService<K, V>>
{

    @Inject
    private ConcurrentMap<K, Pointer<V>> map;

    @Inject
    private MemoryManagerService<V> memoryManager;

    @Inject
    private Serializer serializer;

    public void setMap( ConcurrentMap<K, Pointer<V>> map )
    {
        this.map = map;
    }

    public void setMemoryManager( MemoryManagerService<V> memoryManager )
    {
        this.memoryManager = memoryManager;
    }

    public void setSerializer( Serializer serializer )
    {
        this.serializer = serializer;
    }

    @Override
    public CacheService<K, V> get()
    {
        return new CacheServiceImpl<K, V>( map, memoryManager, serializer );
    }

}
