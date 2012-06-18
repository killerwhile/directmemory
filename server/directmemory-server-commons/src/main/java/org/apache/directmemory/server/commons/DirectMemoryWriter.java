package org.apache.directmemory.server.commons;

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

import org.apache.directmemory.serialization.Serializer;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;

/**
 * @author Olivier Lamy
 */
public class DirectMemoryWriter
{
    private JsonFactory jsonFactory;

    private static final DirectMemoryWriter INSTANCE = new DirectMemoryWriter();

    private Logger log = LoggerFactory.getLogger( getClass() );

    public static DirectMemoryWriter instance()
    {
        return INSTANCE;
    }

    private DirectMemoryWriter()
    {
        this.jsonFactory = new JsonFactory();
    }

    public String generateJsonRequest( DirectMemoryRequest request )
        throws DirectMemoryException
    {
        // TODO configure a minimum size for the writer
        StringWriter stringWriter = new StringWriter();

        try
        {
            JsonGenerator g = this.jsonFactory.createJsonGenerator( stringWriter );

            g.writeStartObject();

            g.writeStringField( DirectMemoryConstants.KEY_FIELD_NAME, request.getKey() );

            g.writeBooleanField( DirectMemoryConstants.PUT_FIELD_NAME, request.isUpdate() );

            g.writeNumberField( DirectMemoryConstants.EXPIRES_IN_FIELD_NAME, request.getExpiresIn() );

            // FIXME take care of NPE
            // cache content generation
            Serializer serializer = request.getSerializer();
            // if no Object users are able to pass a string content
            byte[] bytes = request.getObject() != null
                ? request.getSerializer().serialize( request.getObject() )
                : request.getCacheContent();

            g.writeFieldName( DirectMemoryConstants.CACHE_CONTENT_FIELD_NAME );
            g.writeBinary( bytes );

            if ( serializer != null )
            {
                g.writeStringField( DirectMemoryConstants.SERIALIZER_FIELD_NAME, serializer.getClass().getName() );
            }

            g.writeEndObject();
            g.close();
        }
        catch ( IOException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }

        return stringWriter.toString();
    }

    public String generateJsonResponse( DirectMemoryResponse response )
        throws DirectMemoryException
    {

        // TODO configure a minimum size for the writer
        StringWriter stringWriter = new StringWriter();

        try
        {

            JsonGenerator g = this.jsonFactory.createJsonGenerator( stringWriter );

            g.writeStartObject();

            g.writeBooleanField( DirectMemoryConstants.FOUND_FIELD_NAME, response.isFound() );

            g.writeStringField( DirectMemoryConstants.KEY_FIELD_NAME, response.getKey() );

            if ( response.getCacheContent() != null && response.getCacheContent().length > 0 )
            {
                g.writeFieldName( DirectMemoryConstants.CACHE_CONTENT_FIELD_NAME );
                g.writeBinary( response.getCacheContent() );
            }

            g.writeEndObject();
            g.close();

        }
        catch ( IOException e )
        {
            throw new DirectMemoryException( e.getMessage(), e );
        }

        return stringWriter.toString();

    }
}
