/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public class StringDataBlock extends AbstractDataBlock<String[]> {

	protected static final Charset ENCODING = StandardCharsets.UTF_8;
	protected static final String NULLCHAR = "\0";
	protected byte[] serializedData;
	protected String[] actualData;

	public StringDataBlock(final int[] size, final long[] gridPosition, final String[] data) {
		super(size, gridPosition, null);
		actualData = data;
	}

	@Override
	public void readData(final ByteOrder byteOrder, final ReadData readData) throws IOException {
		serializedData = readData.allBytes();
		final String rawChars = new String(serializedData, ENCODING);
		actualData = rawChars.split(NULLCHAR);
	}

	private byte[] serialize() {
		if (serializedData == null) {
			final String flattenedArray = String.join(NULLCHAR, actualData) + NULLCHAR;
			serializedData = flattenedArray.getBytes(ENCODING);
		}
		return serializedData;
	}

	@Override
	public ReadData writeData(final ByteOrder byteOrder) {
		return ReadData.from(serialize());
	}

	@Override
	public int getNumElements() {
		return serialize().length;
	}

	@Override
	public String[] getData() {
		return actualData;
	}







	static class DefaultCodec implements DataCodec<String[]> {
		@Override
		public ReadData serialize(final DataBlock<String[]> dataBlock) throws IOException {
			final ByteBuffer serialized = ByteBuffer.allocate(Short.BYTES * dataBlock.getNumElements());
			serialized.order(ByteOrder.BIG_ENDIAN).asShortBuffer().put(dataBlock.getData());
			return ReadData.from(serialized);
		}

		@Override
		public void deserialize(final ReadData readData, final DataBlock<String[]> dataBlock) throws IOException {
			readData.toByteBuffer().order(ByteOrder.BIG_ENDIAN).asShortBuffer().get(dataBlock.getData());
		}

		static DefaultCodec INSTANCE = new DefaultCodec();
	}

	@Override
	public DataCodec<String[]> getDataCodec() {
		return DefaultCodec.INSTANCE;
	}

}
