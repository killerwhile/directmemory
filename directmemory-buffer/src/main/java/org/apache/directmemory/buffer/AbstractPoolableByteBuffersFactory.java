package org.apache.directmemory.buffer;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.directmemory.stream.ByteBufferStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractPoolableByteBuffersFactory {

    protected final Logger logger = LoggerFactory.getLogger( this.getClass() );

    private final AtomicBoolean closed = new AtomicBoolean( false );
    
    protected final Logger getLogger()
    {
        return logger;
    }
    
    protected final boolean isClosed()
    {
        return closed.get();
    }

    protected final void setClosed( final boolean closed )
    {
        this.closed.set( closed );
    }
    
}
