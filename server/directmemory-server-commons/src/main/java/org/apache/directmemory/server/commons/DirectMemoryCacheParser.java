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

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import javax.xml.stream.XMLInputFactory;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Olivier Lamy
 */
public class DirectMemoryCacheParser
{
    private XMLInputFactory xmlInputFactory;

    private JsonFactory jsonFactory;

    private static DirectMemoryCacheParser INSTANCE = new DirectMemoryCacheParser();


    private DirectMemoryCacheParser()
    {
        this.xmlInputFactory = XMLInputFactory.newInstance();
        this.xmlInputFactory.setProperty( XMLInputFactory.IS_NAMESPACE_AWARE, Boolean.FALSE );

        this.jsonFactory = new JsonFactory();
    }

    public static DirectMemoryCacheParser instance()
    {
        return INSTANCE;
    }

    public DirectMemoryCacheRequest buildRequest( InputStream inputStream )
        throws DirectMemoryCacheException
    {
        try
        {
            JsonParser jp = this.jsonFactory.createJsonParser( inputStream );
            DirectMemoryCacheRequest rq = new DirectMemoryCacheRequest();
            while ( jp.nextToken() != JsonToken.END_OBJECT )
            {
                String fieldName = jp.getCurrentName();
                if ( DirectMemoryCacheConstants.KEY_ATT_NAME.equals( fieldName ) )
                {
                    rq.setKey( jp.getText() );
                }
                if ( DirectMemoryCacheConstants.PUT_ATT_NAME.equals( fieldName ) )
                {
                    rq.setUpdate( jp.getValueAsBoolean() );
                }
                if ( DirectMemoryCacheConstants.EXPIRES_IN_ATT_NAME.equals( fieldName ) )
                {
                    rq.setExpiresIn( jp.getValueAsInt() );
                }
                if ( DirectMemoryCacheConstants.CACHE_CONTENT_ELEM_NAME.equals( fieldName ) )
                {
                    // binaryValue need to go to nextToken
                    jp.nextToken();
                    rq.setCacheContent( jp.getBinaryValue() );
                }
            }

            jp.close();

            return rq;
        }
        catch ( JsonParseException e )
        {
            throw new DirectMemoryCacheException( e.getMessage(), e );

        }
        catch ( IOException e )
        {
            throw new DirectMemoryCacheException( e.getMessage(), e );
        }
    }

    public DirectMemoryCacheResponse buildResponse( InputStream inputStream )
        throws DirectMemoryCacheException
    {
        try
        {
            JsonParser jp = this.jsonFactory.createJsonParser( inputStream );
            DirectMemoryCacheResponse rs = new DirectMemoryCacheResponse();

            /* <DirectMemoryRS version="1.0" found="" updated="true" key="">
            *   <cacheContent>
            *     <![CDATA[
            *     ]]>
            *   </cacheContent>
            * </DirectMemoryRS>*/

            while ( jp.nextToken() != JsonToken.END_OBJECT )
            {
                String fieldName = jp.getCurrentName();
                if ( DirectMemoryCacheConstants.FOUND_ATT_NAME.equals( fieldName ) )
                {
                    rs.setFound( jp.getValueAsBoolean() );
                }
                if ( DirectMemoryCacheConstants.UPDATED_ATT_NAME.equals( fieldName ) )
                {
                    rs.setUpdated( jp.getValueAsBoolean() );
                }
                if ( DirectMemoryCacheConstants.KEY_ATT_NAME.equals( fieldName ) )
                {
                    rs.setKey( jp.getText() );
                }
                if ( DirectMemoryCacheConstants.CACHE_CONTENT_ELEM_NAME.equals( fieldName ) )
                {
                    // binaryValue need to go to nextToken
                    jp.nextToken();
                    rs.setCacheContent( jp.getBinaryValue() );
                }
            }

            return rs;
        }
        catch ( JsonParseException e )
        {
            throw new DirectMemoryCacheException( e.getMessage(), e );

        }
        catch ( IOException e )
        {
            throw new DirectMemoryCacheException( e.getMessage(), e );
        }
    }

}