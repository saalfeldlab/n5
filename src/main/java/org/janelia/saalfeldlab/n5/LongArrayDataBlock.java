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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongArrayDataBlock extends AbstractDataBlock<long[]> {

	public LongArrayDataBlock(final int[] size, final long[] gridPosition, final long[] data) {

		super(size, gridPosition, data);
	}

	@Override
	public byte[] serialize(final ByteOrder byteOrder) {
		final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * data.length);
		buffer.order(byteOrder).asLongBuffer().put(data);
		return buffer.array();
	}

	@Override
	public void deserialize(final ByteOrder byteOrder, final byte[] serialized) {
		ByteBuffer.wrap(serialized).order(byteOrder).asLongBuffer().get(data);
	}

	@Override
	public void readData(final InputStream inputStream) throws IOException {
		final byte[] bytes = DataType.INT64.createSerializeArray(data.length);
		new DataInputStream(inputStream).readFully(bytes);
		deserialize(bytes);
	}

	@Override
	public int getNumElements() {

		return data.length;
	}
}