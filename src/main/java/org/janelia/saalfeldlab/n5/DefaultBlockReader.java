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

		final ByteBuffer buffer = dataBlock.toByteBuffer();
		try (final InputStream inflater = getInputStream(in)) {
			inflater.read(buffer.array());
		}
		dataBlock.readData(buffer);
	}

	/**
	 * Reads a {@link DataBlock} from an {@link InputStream}.
	 *
	 * @param in
	 * @param datasetAttributes
	 * @param gridPosition
	 * @return
	 * @throws IOException
	 */
	public static DataBlock<?> readBlock(
			final InputStream in,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException {

		final DataInputStream dis = new DataInputStream(in);
		final short mode = dis.readShort();
		final int nDim = dis.readShort();
		final int[] blockSize = new int[nDim];
		for (int d = 0; d < nDim; ++d)
			blockSize[d] = dis.readInt();
		final int numElements;
		switch (mode) {
		case 1:
			numElements = dis.readInt();
			break;
		default:
			numElements = DataBlock.getNumElements(blockSize);
		}
		final DataBlock<?> dataBlock = datasetAttributes.getDataType().createDataBlock(blockSize, gridPosition, numElements);

		final BlockReader reader = datasetAttributes.getCompressionType().getReader();
		reader.read(dataBlock, in);
		return dataBlock;
	}
}