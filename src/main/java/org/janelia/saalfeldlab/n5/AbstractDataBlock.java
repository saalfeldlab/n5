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
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.shard.ShardIndex;

/**
 * Abstract base class for {@link DataBlock} implementations.
 *
 * @param <T>
 *            the block data type
 *
 * @author Stephan Saalfeld
 */
public abstract class AbstractDataBlock<T> implements DataBlock<T> {

	protected final int[] size;
	protected final long[] gridPosition;
	protected final T data;

	public AbstractDataBlock(final int[] size, final long[] gridPosition, final T data) {

		this.size = size;
		this.gridPosition = gridPosition;
		this.data = data;
	}

	@SuppressWarnings("unchecked")
	public DatasetAttributes getDatasetAttributes() {
		return null;
	}

	public ShardIndex getIndex() {

		// TODO alternatively, return a "trivial" index
		return null;
	}

	@Override
	public int[] getSize() {

		return size;
	}

	@Override
	public long[] getGridPosition() {

		return gridPosition;
	}

	@Override
	public T getData() {

		return data;
	}

	public DataBlock<T> getBlock(long... blockGridPosition) {

		if (Arrays.stream(blockGridPosition).anyMatch(x -> x != 0))
			return null;
		else
			return this;
	}

	@Override
	public void readData(final DataInput input) throws IOException {

		final ByteBuffer buffer = toByteBuffer();
		input.readFully(buffer.array());
		readData(buffer);
	}

	@Override
	public void writeData(final DataOutput output) throws IOException {

		final ByteBuffer buffer = toByteBuffer();
		output.write(buffer.array());
	}

}