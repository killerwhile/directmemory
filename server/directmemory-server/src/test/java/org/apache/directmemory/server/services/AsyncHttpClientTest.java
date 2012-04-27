package org.apache.directmemory.server.services;
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

import junit.framework.TestCase;
import org.apache.directmemory.server.client.providers.asynchttpclient.AsyncHttpClientDirectMemoryHttpClient;
import org.apache.directmemory.server.client.providers.httpclient.HttpClientDirectMemoryHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author Olivier Lamy
 */
@RunWith( JUnit4.class )
public class AsyncHttpClientTest
    extends TestCase
{
    @BeforeClass
    public static void setupHttpClientClassName()
    {
        ServletWithClientBinaryTypeTest.httpClientClassName = AsyncHttpClientDirectMemoryHttpClient.class.getName();
        ServletWithClientTextPlainTypeTest.httpClientClassName = AsyncHttpClientDirectMemoryHttpClient.class.getName();
        ServletWithClientJsonTypeTest.httpClientClassName = AsyncHttpClientDirectMemoryHttpClient.class.getName();
    }

    @AfterClass
    public static void restoreHttpClientClassName()
    {
        ServletWithClientBinaryTypeTest.httpClientClassName = HttpClientDirectMemoryHttpClient.class.getName();
        ServletWithClientTextPlainTypeTest.httpClientClassName = HttpClientDirectMemoryHttpClient.class.getName();
        ServletWithClientJsonTypeTest.httpClientClassName = HttpClientDirectMemoryHttpClient.class.getName();
    }

    @Test
    public void testRunAll()
    {

        JUnitCore core = new JUnitCore();
        core.run( ServletWithClientBinaryTypeTest.class );

    }
}
