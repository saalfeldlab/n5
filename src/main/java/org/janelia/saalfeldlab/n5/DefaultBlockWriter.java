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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.janelia.saalfeldlab.n5.codec.Codec.ArrayCodec;
import org.janelia.saalfeldlab.n5.codec.Codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec.DataBlockOutputStream;

/**
 * Default implementation of {@link BlockWriter}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 */
public interface DefaultBlockWriter extends BlockWriter {

	public OutputStream getOutputStream(final OutputStream out) throws IOException;

	@Override
	public default <T> void write(
			final DataBlock<T> dataBlock,
			final OutputStream out) throws IOException {

		final ByteBuffer buffer = dataBlock.toByteBuffer();
		try (final OutputStream deflater = getOutputStream(out)) {
			deflater.write(buffer.array());
			deflater.flush();
		}
	}

	/**
	 * Writes a {@link DataBlock} into an {@link OutputStream}.
	 *
	 * @param <T> the type of data
	 * @param out
	 *            the output stream
	 * @param datasetAttributes
	 *            the dataset attributes
	 * @param dataBlock
	 *            the data block the block data type
	 * @throws IOException
	 *             the exception
	 */
	public static <T> void writeBlock(
			final OutputStream out,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		final BytesCodec[] codecs = datasetAttributes.getCodecs();
		final ArrayCodec arrayCodec = datasetAttributes.getArrayCodec();
		final DataBlockOutputStream dataBlockOutput = arrayCodec.encode(datasetAttributes, dataBlock, out);

		OutputStream stream = dataBlockOutput;
		for (final BytesCodec codec : codecs)
			stream = codec.encode(stream);

		dataBlock.writeData(dataBlockOutput.getDataOutput(stream));
		stream.close();
	}

	public static <T> void writeFromStream(final DataBlock<T> dataBlock, final OutputStream out) throws IOException {

		final ByteBuffer buffer = dataBlock.toByteBuffer();
		out.write(buffer.array());
	}
}