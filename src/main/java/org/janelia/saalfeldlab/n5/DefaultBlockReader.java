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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.janelia.saalfeldlab.n5.codec.Codec.ArrayCodec;
import org.janelia.saalfeldlab.n5.codec.Codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec.DataBlockInputStream;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;

/**
 * Default implementation of {@link BlockReader}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 */
public interface DefaultBlockReader extends BlockReader {

	public InputStream getInputStream(final InputStream in) throws IOException;

	@Override
	public default <T, B extends DataBlock<T>> void read(
			final B dataBlock,
			final InputStream in) throws IOException {

		// do not try with this input stream because subsequent block reads may happen if the stream points to a shard
		final InputStream inflater = getInputStream(in);
		readFromStream(dataBlock, inflater);
	}

	/**
	 * Reads a {@link DataBlock} from an {@link InputStream}.
	 *
	 * @param in
	 *            the input stream
	 * @param datasetAttributes
	 *            the dataset attributes
	 * @param gridPosition
	 *            the grid position
	 * @return the block
	 * @throws IOException
	 *             the exception
	 */
	public static DataBlock<?> readBlock(
			final InputStream in,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException {

		final BytesCodec[] codecs = datasetAttributes.getCodecs();
		final ArrayCodec arrayCodec = datasetAttributes.getArrayCodec();
		final DataBlockInputStream dataBlockStream = arrayCodec.decode(datasetAttributes, gridPosition, in);

		InputStream stream = dataBlockStream;
		for (final BytesCodec codec : codecs) {
			stream = codec.decode(stream);
		}

		final DataBlock<?> dataBlock = dataBlockStream.allocateDataBlock();
		dataBlock.readData(dataBlockStream.getDataInput(stream));
		stream.close();

		return dataBlock;
	}

	public static <T, B extends DataBlock<T>> void readFromStream(final B dataBlock, final InputStream in) throws IOException {

		final ByteBuffer buffer = dataBlock.toByteBuffer();
		final DataInputStream dis = new DataInputStream(in);
		dis.readFully(buffer.array());
		dataBlock.readData(buffer);
	}

	public static long getShardIndex(final ShardingCodec shardingCodec, final long[] gridPosition) {

		// TODO implement
		return -1;
	}

}