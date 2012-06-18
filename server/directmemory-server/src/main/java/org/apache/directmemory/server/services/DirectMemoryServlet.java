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

import org.apache.commons.lang.StringUtils;
import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.DirectMemoryHttpConstants;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Integer.getInteger;
import static org.apache.directmemory.DirectMemory.DEFAULT_CONCURRENCY_LEVEL;
import static org.apache.directmemory.DirectMemory.DEFAULT_INITIAL_CAPACITY;

/**
 * TODO add some listener plugin mechanism to store figures/statistics on cache access
 *
 * @author Olivier Lamy
 */
public class DirectMemoryServlet
    extends HttpServlet
{

    private Logger log = LoggerFactory.getLogger( getClass() );

    private CacheService<Object, Object> cacheService;

    private Map<String, ContentTypeHandler> contentTypeHandlers;


    @Override
    public void init( ServletConfig config )
        throws ServletException
    {
        super.init( config );
        // TODO some configuration for cacheService.init( .... ); different from sysproperties
        //int numberOfBuffers, int size, int initialCapacity, int concurrencyLevel

        cacheService = new DirectMemory<Object, Object>().setNumberOfBuffers(
            getInteger( "directMemory.numberOfBuffers", 10 ) ).setSize(
            getInteger( "directMemory.size", 1000 ) ).setInitialCapacity(
            getInteger( "directMemory.initialCapacity", DEFAULT_INITIAL_CAPACITY ) ).setConcurrencyLevel(
            getInteger( "directMemory.concurrencyLevel", DEFAULT_CONCURRENCY_LEVEL ) ).newCacheService();

        //

        contentTypeHandlers = new HashMap<String, ContentTypeHandler>( 2 );
        contentTypeHandlers.put( MediaType.APPLICATION_JSON, new JsonContentTypeHandler() );
        contentTypeHandlers.put( DirectMemoryHttpConstants.JAVA_SERIALIZED_OBJECT_CONTENT_TYPE_HEADER,
                                 new JavaSerializedContentTypeHandler() );
        contentTypeHandlers.put( MediaType.TEXT_PLAIN, new TextPlainContentTypeHandler() );
        log.info( "DirectMemoryServlet initialized" );

    }

    @Override
    public void destroy()
    {
        super.destroy();
    }

    @Override
    protected void doPost( HttpServletRequest req, HttpServletResponse resp )
        throws ServletException, IOException
    {
        this.doPut( req, resp );
    }

    @Override
    protected void doPut( HttpServletRequest req, HttpServletResponse resp )
        throws ServletException, IOException
    {
        //TODO check request content to send HttpServletResponse.SC_BAD_REQUEST
        // if missing parameter in json request

        String path = req.getPathInfo();
        String servletPath = req.getServletPath();
        String key = retrieveKeyFromPath( path );

        DirectMemoryRequest request = null;

        ContentTypeHandler contentTypeHandler = findPutCacheContentTypeHandler( req, resp );

        if ( contentTypeHandler == null )
        {
            String contentType = req.getContentType();
            log.error( "No content type handler for content type {}", contentType );
            resp.sendError( HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                            "Content-Type '" + contentType + "' not supported" );
            return;
        }
        try
        {
            request = contentTypeHandler.handlePut( req, resp );
        }
        catch ( DirectMemoryException e )
        {
            resp.sendError( HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage() );
            return;
        }

        //if exists free first ?
        //if ( cacheService.retrieveByteArray( key ) == null )
        byte[] bytes = request.getCacheContent();
        Pointer p = cacheService.putByteArray( key, bytes, request.getExpiresIn() );
        if ( p == null )
        {
            resp.sendError( HttpServletResponse.SC_NO_CONTENT, "Content not put in cache for key: " + key );
            return;
        }
        log.debug( "put content for key {} size {}", key, bytes.length );
        resp.addHeader( DirectMemoryHttpConstants.EXPIRES_SERIALIZE_SIZE, Integer.toString( bytes.length ) );
    }

    protected ContentTypeHandler findPutCacheContentTypeHandler( HttpServletRequest req, HttpServletResponse response )
    {

        String contentType = req.getContentType();
        if ( StringUtils.startsWith( contentType, MediaType.APPLICATION_JSON ) )
        {
            // 	application/json
            return contentTypeHandlers.get( MediaType.APPLICATION_JSON );
        }
        else if ( StringUtils.startsWith( contentType, MediaType.TEXT_PLAIN ) )
        {
            // text/plain
            return contentTypeHandlers.get( MediaType.TEXT_PLAIN );
        }
        return contentTypeHandlers.get( contentType );
    }

    @Override
    protected void doDelete( HttpServletRequest req, HttpServletResponse resp )
        throws ServletException, IOException
    {
        String path = req.getPathInfo();
        String servletPath = req.getServletPath();
        String key = retrieveKeyFromPath( path );

        Pointer pointer = cacheService.getPointer( key );
        if ( pointer == null )
        {
            resp.sendError( HttpServletResponse.SC_NO_CONTENT, "No content for key: " + key );
            return;
        }
        cacheService.free( pointer );
        log.debug( "free content of key: {}", key );
    }

    @Override
    protected void doGet( HttpServletRequest req, HttpServletResponse resp )
        throws ServletException, IOException
    {
        // url format = /cache/key so get the key from path
        String path = req.getPathInfo();
        String servletPath = req.getServletPath();
        String key = retrieveKeyFromPath( path );

        if ( StringUtils.isEmpty( key ) )
        {
            resp.sendError( HttpServletResponse.SC_BAD_REQUEST, "key missing in path" );
            return;
        }

        String acceptContentType = req.getHeader( "Accept" );

        if ( StringUtils.isEmpty( acceptContentType ) )
        {
            resp.sendError( HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                            "you must specify Accept with Content-Type you want in the response" );
            return;
        }

        ContentTypeHandler contentTypeHandler = findGetCacheContentTypeHandler( req, resp );

        if ( contentTypeHandler == null )
        {
            String contentType = req.getContentType();
            log.error( "No content type handler for content type {}", acceptContentType );
            resp.sendError( HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                            "Content-Type: " + acceptContentType + " not supported" );
            return;
        }

        byte[] bytes = cacheService.retrieveByteArray( key );

        log.debug( "return content size {} for key {}", ( bytes == null ? "null" : bytes.length ), key );

        if ( bytes == null || bytes.length == 0 )
        {
            resp.sendError( HttpServletResponse.SC_NO_CONTENT, "No content for key: " + key );
            return;
        }

        try
        {
            byte[] respBytes =
                contentTypeHandler.handleGet( new DirectMemoryRequest().setKey( key ), bytes, resp, req );
            resp.getOutputStream().write( respBytes );
        }
        catch ( DirectMemoryException e )
        {
            resp.sendError( HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage() );
        }


    }

    protected ContentTypeHandler findGetCacheContentTypeHandler( HttpServletRequest req, HttpServletResponse response )
    {

        String acceptContentType = req.getHeader( "Accept" );
        if ( StringUtils.contains( acceptContentType, MediaType.APPLICATION_JSON ) )
        {
            // 	application/json
            return contentTypeHandlers.get( MediaType.APPLICATION_JSON );
        }
        else if ( StringUtils.startsWith( acceptContentType, MediaType.TEXT_PLAIN ) )
        {
            return contentTypeHandlers.get( MediaType.TEXT_PLAIN );
        }
        return contentTypeHandlers.get( acceptContentType );
    }

    /**
     * protected for unit test reason
     *
     * @param path
     * @return
     */

    protected String retrieveKeyFromPath( String path )
    {
        if ( StringUtils.endsWith( path, "/" ) )
        {
            return StringUtils.substringAfterLast( StringUtils.substringBeforeLast( path, "/" ), "/" );
        }
        return StringUtils.substringAfterLast( path, "/" );
    }
}
