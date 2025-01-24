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

import java.io.BufferedInputStream;
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

	@Deprecated
	public InputStream getInputStream(final InputStream in) throws IOException;

	@Deprecated
	@Override
	public default <T, B extends DataBlock<T>> void read(
			final B dataBlock,
			final InputStream in) throws IOException {

		final ByteBuffer buffer = dataBlock.toByteBuffer();
		try (final InputStream inflater = getInputStream(in)) {
			final DataInputStream dis = new DataInputStream(inflater);
			dis.readFully(buffer.array());
		}
		dataBlock.readData(buffer);
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

		final DataType dataType = datasetAttributes.getDataType();
		final DataInputStream dis = new DataInputStream(in);
		final short mode = dis.readShort();
		final int numElements;
		final DataBlock<?> dataBlock;
		if (mode != 2) {
			final int nDim = dis.readShort();
			final int[] blockSize = new int[nDim];
			for (int d = 0; d < nDim; ++d)
				blockSize[d] = dis.readInt();
			if (mode == 0) {
				numElements = DataBlock.getNumElements(blockSize);
			} else {
				numElements = dis.readInt();
			}
			dataBlock = dataType.createDataBlock(blockSize, gridPosition, numElements);
		} else {
			numElements = dis.readInt();
			dataBlock = dataType.createDataBlock(null, gridPosition, numElements);
		}

		// variant 1
		final byte[] data = dataType.createSerializeArray(numElements);
		try (final InputStream inflater = datasetAttributes.getCompression().decode(in)) {
			final DataInputStream dis2 = new DataInputStream(inflater);
			dis2.readFully(data);
		}
		dataBlock.deserialize(data);

		// variant 2
//		final byte[] data = datasetAttributes.getCompression().decode(Java9StreamMethods.readAllBytes(in));
//		dataBlock.deserialize(data);

		// old
//		final BlockReader reader = datasetAttributes.getCompression().getReader();
//		reader.read(dataBlock, in);

		return dataBlock;
	}
}