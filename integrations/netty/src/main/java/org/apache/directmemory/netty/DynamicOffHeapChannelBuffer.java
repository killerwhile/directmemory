package org.apache.directmemory.netty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import org.apache.directmemory.buffer.PoolableByteBuffersFactory;
import org.apache.directmemory.stream.ByteBufferInputStream;
import org.apache.directmemory.stream.ByteBufferOutputStream;
import org.apache.directmemory.stream.ByteBufferStream;
import org.jboss.netty.buffer.AbstractChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;

public class DynamicOffHeapChannelBuffer extends AbstractChannelBuffer {

	private final PoolableByteBuffersFactory factory;
	
	private final ByteBufferStream stream;
	
	private final ByteBufferOutputStream out;
	private final ByteBufferInputStream in;
	
	private final ByteOrder order;
	
	public DynamicOffHeapChannelBuffer(final PoolableByteBuffersFactory factory) {
		this(factory, ByteOrder.nativeOrder());
	}
	
	public DynamicOffHeapChannelBuffer(final PoolableByteBuffersFactory factory, final ByteOrder order) {
		
		this.factory = factory;
		
		this.stream = this.factory.getInOutStream();
		
		this.out = stream.getOutputStream();
		this.in = stream.getInputStream();
		
		this.order = order;
	}
	
	@Override
	public byte[] array() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int arrayOffset() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int capacity() {
		return stream.getCurrentCapacity();
	}

	@Override
	public ChannelBuffer copy(int arg0, int arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelBuffer duplicate() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelBufferFactory factory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte getByte(int arg0) {
		return in.readByte(arg0);
	}

	@Override
	public void getBytes(int arg0, ByteBuffer arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void getBytes(int arg0, OutputStream arg1, int arg2)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getBytes(int arg0, GatheringByteChannel arg1, int arg2)
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void getBytes(int arg0, ChannelBuffer arg1, int arg2, int arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void getBytes(int arg0, byte[] arg1, int arg2, int arg3) {
		in.readBytes(arg0, arg1, arg2, arg3);
	}

	@Override
	public int getInt(int arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(int arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short getShort(int arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getUnsignedMedium(int arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean hasArray() {
		return false;
	}

	@Override
	public boolean isDirect() {
		return true;
	}

	@Override
	public ByteOrder order() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setByte(int arg0, int arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setBytes(int arg0, ByteBuffer arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int setBytes(int arg0, InputStream arg1, int arg2)
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int setBytes(int arg0, ScatteringByteChannel arg1, int arg2)
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setBytes(int arg0, ChannelBuffer arg1, int arg2, int arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setBytes(int arg0, byte[] arg1, int arg2, int arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setInt(int arg0, int arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLong(int arg0, long arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setMedium(int arg0, int arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setShort(int arg0, int arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ChannelBuffer slice(int arg0, int arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteBuffer toByteBuffer(int arg0, int arg1) {
		// TODO Auto-generated method stub
		return null;
	}

}
