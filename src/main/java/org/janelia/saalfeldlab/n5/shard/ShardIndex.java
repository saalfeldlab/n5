package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.SplittableReadData;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.stream.IntStream;

public class ShardIndex extends LongArrayDataBlock {

	public static final long EMPTY_INDEX_NBYTES = 0xFFFFFFFFFFFFFFFFL;
	private static final int BYTES_PER_LONG = 8;
	private static final int LONGS_PER_BLOCK = 2;
	private static final long[] DUMMY_GRID_POSITION = null;

	private final IndexLocation location;

	private final DeterministicSizeCodec[] codecs;
	private final ShardIndexAttributes indexAttributes;

	public ShardIndex(int[] shardBlockGridSize, long[] data, IndexLocation location, final DeterministicSizeCodec... codecs) {

		super(prepend(LONGS_PER_BLOCK, shardBlockGridSize), DUMMY_GRID_POSITION, data);
		this.codecs = codecs;
		this.location = location;
		this.indexAttributes = new ShardIndexAttributes(this);
	}

	public ShardIndex(int[] shardBlockGridSize, IndexLocation location, DeterministicSizeCodec... codecs) {

		this(shardBlockGridSize, emptyIndexData(shardBlockGridSize), location, codecs);
	}

	public ShardIndex(int[] shardBlockGridSize, DeterministicSizeCodec... codecs) {

		this(shardBlockGridSize, emptyIndexData(shardBlockGridSize), IndexLocation.END, codecs);
	}

	public boolean exists(int[] gridPosition) {

		return getOffset(gridPosition) != EMPTY_INDEX_NBYTES ||
				getNumBytes(gridPosition) != EMPTY_INDEX_NBYTES;
	}

	public boolean exists(int blockNum) {

		return data[blockNum * 2] != EMPTY_INDEX_NBYTES ||
				data[blockNum * 2 + 1] != EMPTY_INDEX_NBYTES;
	}

	public int getNumBlocks() {

		/* getSize() is the number of data entries; each block takes 2 entries (offset and length)
		* so the product of the dimension sizes, divided by 2, is the number of blocks. */
		return Arrays.stream(getSize()).reduce(1, (x, y) -> x * y) / 2;
	}

	public boolean isEmpty() {

		return !IntStream.range(0, getNumBlocks()).anyMatch(this::exists);
	}

	public IndexLocation getLocation() {

		return location;
	}

	public long getOffset(int... gridPosition) {

		return data[getOffsetIndex(gridPosition)];
	}

	public long getOffsetByBlockIndex(int index) {

		return data[index * 2];
	}

	public long getNumBytes(int... gridPosition) {

		return data[getNumBytesIndex(gridPosition)];
	}

	public long getNumBytesByBlockIndex(int index) {

		return data[index * 2 + 1];
	}

	public void set(long offset, long nbytes, int[] gridPosition) {

		final int i = getOffsetIndex(gridPosition);
		data[i] = offset;
		data[i + 1] = nbytes;
	}

	protected int getOffsetIndex(int... gridPosition) {

		int idx = (int) gridPosition[0];
		int cumulativeSize = 1;
		for (int i = 1; i < gridPosition.length; i++) {
			cumulativeSize *= size[i];
			idx += gridPosition[i] * cumulativeSize;
		}
		return idx * 2;
	}

	protected int getNumBytesIndex(int... gridPosition) {

		return getOffsetIndex(gridPosition) + 1;
	}

	public long numBytes() {

		final int numEntries = Arrays.stream(getSize()).reduce(1, (x, y) -> x * y);
		final int numBytesFromBlocks = numEntries * BYTES_PER_LONG;
		long totalNumBytes = numBytesFromBlocks;
		for (Codec codec : codecs) {
			if (codec instanceof DeterministicSizeCodec) {
				totalNumBytes = ((DeterministicSizeCodec)codec).encodedSize(totalNumBytes);
			}
		}
		return totalNumBytes;
	}

	public static void readFromShard(ReadData shardData, ShardIndex index) throws N5IOException {

		/* we require a length, so materialize if we don't have one. */
		if (shardData.length() == -1)
			shardData.materialize();

		final long length = shardData.length();
		if (length == -1)
			throw new N5IOException("ReadData for shard index must have a valid length, but was " + length);

		final ShardIndex.IndexByteBounds bounds = ShardIndex.byteBounds(index, length);
		final ReadData indexData;
		try {
			indexData = ((SplittableReadData)shardData).slice(bounds.start, index.numBytes());
		} catch (IOException e) {
			throw new N5IOException("Failed to read shard index", e);
		}
		ShardIndex.read(indexData, index);
	}

	public static boolean read( final ReadData indexData, final ShardIndex index ) {

		try (final InputStream in = indexData.inputStream()) {
			read(in, index);
			return true;
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return false;
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read shard index", e);
		}
	}

	public static void read(InputStream indexIn, final ShardIndex index) throws N5IOException {

		final ReadData dataIn = ReadData.from(indexIn);
		final Codec.ArrayCodec<long[]> shardIndexCodec = index.indexAttributes.getArrayCodec();
		final DataBlock<long[]> indexBlock = shardIndexCodec.decode(dataIn, index.gridPosition);
		System.arraycopy(indexBlock.getData(), 0, index.data, 0, index.data.length);
	}

	public static void write( final OutputStream outputStream, final ShardIndex index ) throws N5IOException {

		final Codec.ArrayCodec<long[]> indexCodec = index.indexAttributes.getArrayCodec();
		indexCodec.encode(index).writeTo(outputStream);
	}


	public Codec.ArrayCodec<?> getArrayCodec() {
		return indexAttributes.getArrayCodec();
	}

	private static class ShardIndexAttributes extends DatasetAttributes {

		public ShardIndexAttributes(ShardIndex index) {
			super(
					Arrays.stream(index.getSize()).mapToLong(it -> it).toArray(),
					index.getSize(),
					DataType.UINT64,
					index.codecs
					);
		}
	}

	public static IndexByteBounds byteBounds(final ShardIndex index, long objectSize) {

		return byteBounds(index.numBytes(), index.location, objectSize);
	}

	public static IndexByteBounds byteBounds(final long indexSize, final IndexLocation indexLocation, final long objectSize) {

		if (indexLocation == IndexLocation.START) {
			return new IndexByteBounds(0L, indexSize);
		} else {
			return new IndexByteBounds(objectSize - indexSize, objectSize - 1);
		}
	}

	public static class IndexByteBounds {

		public final long start;
		public final long end;
		public final long size;

		public IndexByteBounds(long start, long end) {

			this.start = start;
			this.end = end;
			this.size = end - start + 1;
		}
	}

	private static long[] emptyIndexData(final int[] size) {

		final int N = 2 * Arrays.stream(size).reduce(1, (x, y) -> x * y);
		final long[] data = new long[N];
		Arrays.fill(data, EMPTY_INDEX_NBYTES);
		return data;
	}

	private static int[] prepend(final int value, final int[] array) {

		final int[] indexBlockSize = new int[array.length + 1];
		indexBlockSize[0] = value;
		System.arraycopy(array, 0, indexBlockSize, 1, array.length);
		return indexBlockSize;
	}

	@Override
	public boolean equals(Object other) {

		if (other instanceof ShardIndex) {

			final ShardIndex index = (ShardIndex)other;
			if (this.location != index.location)
				return false;

			if (!Arrays.equals(this.size, index.size))
				return false;

			if (!Arrays.equals(this.data, index.data))
				return false;

		}
		return true;
	}

	public static ShardIndex createIndex(final DatasetAttributes attributes) {

		return attributes.getShardingCodec().createIndex(attributes);
	}


}

