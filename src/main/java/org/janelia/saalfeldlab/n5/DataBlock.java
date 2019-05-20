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

import java.nio.ByteBuffer;

/**
 * Interface for data blocks.  A data block has data, a position on the block
 * grid, a size, and can read itself from and write itself into a
 * {@link ByteBuffer}.
 *
 * @param <T>
 *
 * @author Stephan Saalfeld
 */
public interface DataBlock<T> {

	/**
	 * Returns the size of this data block.
	 *
	 * The size of a data block is expected to be smaller than or equal to the
	 * spacing of the block grid.  The dimensionality of size is expected to be
	 * equal to the dimensionality of the dataset.  Consistency is not enforced.
	 *
	 * @return size of the data block
	 */
	public int[] getSize();

	/**
	 * Returns the position of this data block on the block grid.
	 *
	 * The dimensionality of the grid position is expected to be equal to the
	 * dimensionality of the dataset.  Consistency is not enforced.
	 *
	 * @return position on the block grid
	 */
	public long[] getGridPosition();

	/**
	 * Returns the data object held by this data block.
	 *
	 * @return data object
	 */
	public T getData();

	/**
	 * Creates a {@link ByteBuffer} that contains the data object of this data
	 * block.
	 *
	 * The {@link ByteBuffer} may or may not map directly to the data
	 * object of this data block.  I.e. modifying the {@link ByteBuffer} after
	 * calling this method may or may not change the data of this data block.
	 * modifying the data object of this data block after calling this method
	 * may or may not change the content of the {@link ByteBuffer}.
	 *
	 * @return {@link ByteBuffer} containing data
	 */
	public ByteBuffer toByteBuffer();

	/**
	 * Reads the data object of this data block from a {@link ByteBuffer}.
	 *
	 * The {@link ByteBuffer} may or may not map directly to the data
	 * object of this data block.  I.e. modifying the {@link ByteBuffer} after
	 * calling this method may or may not change the data of this data block.
	 * modifying the data object of this data block after calling this method
	 * may or may not change the content of the {@link ByteBuffer}.
	 *
	 * @param buffer
	 */
	public void readData(final ByteBuffer buffer);

	/**
	 * Returns the number of elements in this {@link DataBlock}. This number is
	 * not necessarily equal {@link #getNumElements(int[])
	 * getNumElements(getSize())}.
	 *
	 * @return
	 */
	public int getNumElements();

	/**
	 * Returns the number of elements in a box of given size.
	 *
	 * @param size
	 * @return
	 */
	public static int getNumElements(final int[] size) {
		int n = size[0];
		for (int i = 1; i < size.length; ++i)
			n *= size[i];
		return n;
	}
}