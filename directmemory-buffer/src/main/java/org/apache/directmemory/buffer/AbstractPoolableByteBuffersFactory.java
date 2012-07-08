package org.apache.directmemory.buffer;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractPoolableByteBuffersFactory 
{

	// max value of ByteBuffer.allocateDirect(). Trying to allocate segments 
	// bigger than this value will be resized to not overtake this value.
    protected static final long MAX_SEGMENT_SIZE = Integer.MAX_VALUE / 2;
    
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
