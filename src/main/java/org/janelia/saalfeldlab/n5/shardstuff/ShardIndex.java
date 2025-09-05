package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.segment.Segment;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentLocation;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentedReadData;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentedReadData.SegmentsAndData;

public class ShardIndex {

	static class Flattened<T> {

		final int[] size;
		private final int[] stride;
		final T[] data;

		Flattened(final int[] size, final IntFunction<T[]> createArray) {
			this.size = size;
			stride = stride(size);
			data = createArray.apply(numElements(size));
		}

		Flattened(final int[] size, final T[] data) {
			this.size = size;
			stride = stride(size);
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
	}

	static int numElements(final int[] size) {
		int numElements = 1;
		for (int s : size) {
			numElements *= s;
		}
		return numElements;
	}

	static int[] stride(final int[] size) {
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

	// TODO do we need additional offset here?
	static Flattened<SegmentLocation> fromDataBlock( final DataBlock<long[]> block ) {

		assert block.getSize()[ 0 ] == 2;

		final int[] blockSize = block.getSize();
		final long[] blockData = block.getData();

		final int[] size = Arrays.copyOfRange(blockSize, 1, blockSize.length);
		final int n = numElements(size);
		final SegmentLocation[] locations = new SegmentLocation[n];

		for (int i = 0; i < n; i++) {
			long offset = blockData[ i * 2];
			long length = blockData[ i * 2 + 1];
			if (offset != EMPTY_INDEX_NBYTES && length != EMPTY_INDEX_NBYTES) {
				locations[i] = SegmentLocation.at(offset, length);
			}
		}
		return new Flattened<>(size, locations);
	}

	// TODO do we use offset? If not, remove!
	static DataBlock<long[]> toDataBlock( final Flattened<SegmentLocation> locations, final long offset ) {

		final SegmentLocation[] data = locations.data;

		final int[] blockSize = prepend(2, locations.size);
		final long[] blockData = new long[data.length * 2];

		for (int i = 0; i < data.length; ++i) {
			if (data[i] != null) {
				blockData[i * 2] = data[i].offset() + offset;
				blockData[i * 2 + 1] = data[i].length();
			} else {
				blockData[i * 2] = EMPTY_INDEX_NBYTES;
				blockData[i * 2 + 1] = EMPTY_INDEX_NBYTES;
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

	static Flattened<SegmentLocation> locations(final Flattened<Segment> segments, final SegmentedReadData readData) {

		final Segment[] data = segments.data;
		final SegmentLocation[] locations = new SegmentLocation[data.length];
		for (int i = 0; i < data.length; ++i) {
			final Segment segment = data[i];
			if ( segment != null ) {
				locations[i] = readData.location(segment);
			}
		}
		return new Flattened<>(segments.size, locations);
	}

	interface SegmentIndexAndData {
		Flattened<Segment> index();
		SegmentedReadData data();
	}

	static SegmentIndexAndData segments(final Flattened<SegmentLocation> locations, final ReadData readData) {

		final SegmentLocation[] locationsData = locations.data;
		final Segment[] segmentsData = new Segment[locationsData.length];

		final List<SegmentLocation> presentLocations = new ArrayList<>();
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

		final Flattened<Segment> index = new Flattened<>(locations.size, segmentsData);
		final SegmentedReadData data = segmentsAndData.data();
		return new SegmentIndexAndData() {
			@Override public Flattened<Segment> index() {return index;}
			@Override public SegmentedReadData data() {return data;}
		};
	}
}
