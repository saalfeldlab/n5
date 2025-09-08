/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * De/serialize {@link DataBlock} from/to {@link ReadData}.
 *
 * @param <T>
 * 		type of the data contained in the DataBlock
 */
public interface BlockCodec<T> {

	ReadData encode(DataBlock<T> dataBlock) throws N5IOException;

	DataBlock<T> decode(ReadData readData, long[] gridPosition) throws N5IOException;

	/**
	 * Given the {@code blockSize} of a {@code DataBlock<T>} return the size of
	 * the encoded block in bytes.
	 * <p>
	 * A {@code UnsupportedOperationException} is thrown, if this {@code
	 * BlockCodec} cannot determine encoded size independent of block content.
	 * For example, if the block type contains var-length elements or if the
	 * serializer uses a non-deterministic {@code DataCodec}.
	 *
	 * @param blockSize
	 * 		size of the block to be encoded
	 *
	 * @return size of the encoded block in bytes
	 *
	 * @throws UnsupportedOperationException
	 * 		if this {@code DataBlockSerializer} cannot determine encoded size independent of block content
	 */
	default long encodedSize(int[] blockSize) throws UnsupportedOperationException {

		// TODO: Adapt https://github.com/saalfeldlab/n5/pull/165 to new naming!

		// TODO: REPLACE! This is a dirty hack, assuming this is only called for
		//       ShardIndex (UINT64) and the ShardIndex uses RawBlockCodec and
		//       RawCompression.
		return DataBlock.getNumElements(blockSize) * 8L;
	}
}
