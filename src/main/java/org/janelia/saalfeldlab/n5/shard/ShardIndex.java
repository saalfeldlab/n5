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
package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.segment.Segment;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentedReadData;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentedReadData.SegmentsAndData;

import com.google.gson.annotations.SerializedName;

public class ShardIndex {

	private ShardIndex() {
		// utility class. should not be instantiated.
	}

	public enum IndexLocation {
		@SerializedName("start") START,
		@SerializedName("end") END
	}

	/**
	 * Access flat {@code T[]} array as n-dimensional array.
	 *
	 * @param <T>
	 * 		element type
	 */
	public static class NDArray<T> {

		final int[] size;
		private final int[] stride;
		final T[] data;

		NDArray(final int[] size, final IntFunction<T[]> createArray) {
			this.size = size;
			stride = getStrides(size);
			data = createArray.apply(getNumElements(size));
		}

		NDArray(final int[] size, final T[] data) {
			this.size = size;
			stride = getStrides(size);
			this.data = data;
		}

		T get(long... position) {
			return data[index(position)];
		}

		void set(T value, long... position) {
			data[index(position)] = value;
		}

		private int index(long... position) {
			int index = 0;
			for (int i = 0; i < stride.length; i++) {
				index += stride[i] * position[i];
			}
			return index;
		}

		public int[] size() {
			return size;
		}

		public int numElements() {
			return data.length;
		}

		public boolean allElementsNull() {
			for (T t : data) {
				if (t != null) {
					return false;
				}
			}
			return true;
		}
	}

	static int getNumElements(final int[] size) {
		int numElements = 1;
		for (int s : size) {
			numElements *= s;
		}
		return numElements;
	}

	static int[] getStrides(final int[] size) {
		final int n = size.length;
		final int[] stride = new int[n];
		stride[0] = 1;
		for (int i = 1; i < n; i++) {
			stride[i] = stride[i - 1] * size[i - 1];
		}
		return stride;
	}

	/**
	 * Special value indicating an empty block entry in the index.
	 * Used for both offset and length when a block doesn't exist.
	 */
	static final long EMPTY_INDEX_NBYTES = 0xFFFFFFFFFFFFFFFFL;

	/**
	 * Size of first dimension of the {@code DataBlock<long[]>} representation of the shard index.
	 */
	private static final int LONGS_PER_BLOCK = 2;

	static NDArray<Range> fromDataBlock( final DataBlock<long[]> block ) {

		final long[] blockData = block.getData();
		final int[] size = indexSizeFromBlockSize(block.getSize());
		final int n = getNumElements(size);
		final Range[] locations = new Range[n];

		for (int i = 0; i < n; i++) {
			long offset = blockData[i * LONGS_PER_BLOCK];
			long length = blockData[i * LONGS_PER_BLOCK + 1];
			if (offset != EMPTY_INDEX_NBYTES && length != EMPTY_INDEX_NBYTES) {
				locations[i] = Range.at(offset, length);
			}
		}
		return new NDArray<>(size, locations);
	}

	static DataBlock<long[]> toDataBlock( final NDArray<Range> locations, final long offset ) {

		final Range[] data = locations.data;

		final int[] blockSize = blockSizeFromIndexSize(locations.size);
		final long[] blockData = new long[data.length * 2];

		for (int i = 0; i < data.length; ++i) {
			if (data[i] != null) {
				blockData[i * LONGS_PER_BLOCK] = data[i].offset() + offset;
				blockData[i * LONGS_PER_BLOCK + 1] = data[i].length();
			} else {
				blockData[i * LONGS_PER_BLOCK] = EMPTY_INDEX_NBYTES;
				blockData[i * LONGS_PER_BLOCK + 1] = EMPTY_INDEX_NBYTES;
			}
		}
		return new LongArrayDataBlock(blockSize, new long[blockSize.length], blockData);
	}

	/**
	 * Prepends a value to an array.
	 *
	 * @param value the value to prepend
	 * @param array the original array
	 * @return a new array with the value prepended
	 */
	private static int[] prepend(final int value, final int[] array) {

		final int[] indexBlockSize = new int[array.length + 1];
		indexBlockSize[0] = value;
		System.arraycopy(array, 0, indexBlockSize, 1, array.length);
		return indexBlockSize;
	}

	/**
	 * Prepends {@code LONGS_PER_BLOCK} to the {@code indexSize} array.
	 */
	static int[] blockSizeFromIndexSize(final int[] indexSize) {
		return prepend(LONGS_PER_BLOCK, indexSize);
	}

	/**
	 * Strips first element (should be {@code LONGS_PER_BLOCK}) from the {@code blockSize} array.
	 */
	static int[] indexSizeFromBlockSize(final int[] blockSize) {
		assert blockSize[ 0 ] == LONGS_PER_BLOCK;
		return Arrays.copyOfRange(blockSize, 1, blockSize.length);
	}

	/**
	 * Retrieves the {@code SegmentLocation} of each non-null {@code Segment} in
	 * {@code segments}. Returns a {@code NDArray<SegmentLocation>} with entries
	 * corresponding tho the {@code segments} entries.
	 */
	static NDArray<Range> locations(final NDArray<Segment> segments, final SegmentedReadData readData) {

		final Segment[] data = segments.data;
		final Range[] locations = new Range[data.length];
		for (int i = 0; i < data.length; ++i) {
			final Segment segment = data[i];
			if ( segment != null ) {
				locations[i] = readData.location(segment);
			}
		}
		return new NDArray<>(segments.size, locations);
	}

	interface SegmentIndexAndData {
		NDArray<Segment> index();
		SegmentedReadData data();
	}

	/**
	 * Puts a {@code Segment} at each non-null {@code SegmentLocation} in {@code
	 * locations} on the given {@code readData}. Returns both the {@code
	 * SegmentedReadData} with these segments and a {@code NDArray<Segment>}
	 * with segment entries corresponding to the {@code locations} entries.
	 */
	static SegmentIndexAndData segments(final NDArray<Range> locations, final ReadData readData) {

		final Range[] locationsData = locations.data;
		final Segment[] segmentsData = new Segment[locationsData.length];

		final List<Range> presentLocations = new ArrayList<>();
		for (int i = 0; i < locationsData.length; i++) {
			if (locationsData[i] != null) {
				presentLocations.add(locationsData[i]);
			}
		}

		final SegmentsAndData segmentsAndData = SegmentedReadData.wrap(readData, presentLocations);
		final Iterator<Segment> presentSegments = segmentsAndData.segments().iterator();
		for (int i = 0; i < locationsData.length; i++) {
			if (locationsData[i] != null) {
				segmentsData[i] = presentSegments.next();
			}
		}

		final NDArray<Segment> index = new NDArray<>(locations.size, segmentsData);
		final SegmentedReadData data = segmentsAndData.data();
		return new SegmentIndexAndData() {
			@Override public NDArray<Segment> index() {return index;}
			@Override public SegmentedReadData data() {return data;}
		};
	}
}
