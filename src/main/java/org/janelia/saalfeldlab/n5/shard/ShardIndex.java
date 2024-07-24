package org.janelia.saalfeldlab.n5.shard;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.shard.ShardingConfiguration.IndexLocation;

public class ShardIndex extends LongArrayDataBlock {

	private static final int BYTES_PER_LONG = 8;

	private static final int LONGS_PER_BLOCK = 2;

	private static final long[] DUMMY_GRID_POSITION = null;

	public ShardIndex(int[] shardBlockGridSize, long[] data) {

		super(prepend(LONGS_PER_BLOCK, shardBlockGridSize), DUMMY_GRID_POSITION, data);
	}

	public ShardIndex(int[] shardBlockGridSize) {

		super(prepend(LONGS_PER_BLOCK, shardBlockGridSize), DUMMY_GRID_POSITION, emptyIndexData(shardBlockGridSize));
	}

	public long getOffset(long... gridPosition) {

		return data[getOffsetIndex(gridPosition)];
	}

	public long getNumBytes(long... gridPosition) {

		return data[getNumBytesIndex(gridPosition)];
	}

	public void set(long offset, long nbytes, long[] gridPosition) {

		final int i = getOffsetIndex(gridPosition);
		data[i] = offset;
		data[i + 1] = nbytes;
	}

	private int getOffsetIndex(long... gridPosition) {

		int idx = 0;
		long stride = 2;
		for (int i = 0; i < gridPosition.length; i++) {
			idx += gridPosition[i] * stride;
			stride *= size[i];
		}

		return idx;
	}

	private int getNumBytesIndex(long... gridPosition) {

		return getOffsetIndex() + 1;
	}

	public static ShardIndex read(final KeyValueAccess keyValueAccess, final String key,
			final ShardedDatasetAttributes datasetAttributes) throws IOException {

		return read(keyValueAccess, key, datasetAttributes.getShardBlockGridSize(),
				datasetAttributes.getIndexLocation());
	}

	public static ShardIndex read(final KeyValueAccess keyValueAccess, final String key, final int[] shardBlockGridSize,
			final IndexLocation indexLocation) throws IOException {

		final ShardIndex idx = new ShardIndex(shardBlockGridSize);
		final IndexByteBounds byteBounds = byteBounds(idx.getSize(), indexLocation, keyValueAccess.size(key));
		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(key, byteBounds.start, byteBounds.end)) {

			final byte[] bytes = new byte[idx.getNumElements() * ShardIndex.BYTES_PER_LONG];
			lockedChannel.newInputStream().read(bytes);
			idx.readData(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)); // TODO generalize byte order
			return idx;

		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read from " + key, e);
		}
	}

	public static DatasetAttributes indexDatasetAttributes(final int[] indexBlockSize) {

		final int[] blkSize = new int[indexBlockSize.length];
		final long[] size = new long[indexBlockSize.length];
		for (int i = 0; i < blkSize.length; i++) {
			blkSize[i] = (int)indexBlockSize[i];
		}

		// TODO codecs
		return new DatasetAttributes(size, blkSize, DataType.UINT64, new RawCompression(), null);
	}

	public static IndexByteBounds byteBounds(ShardedDatasetAttributes datasetAttributes, final long objectSize) {

		final int[] indexShape = prepend(2, datasetAttributes.getShardBlockGridSize());
		return byteBounds(indexShape, datasetAttributes.getIndexLocation(), objectSize);
	}

	public static IndexByteBounds byteBounds(final int[] indexShape, final IndexLocation indexLocation,
			final long objectSize) {

		final int indexSize = (int)Arrays.stream(indexShape).reduce(1, (x, y) -> x * y);

		if (indexLocation == IndexLocation.START) {
			return new IndexByteBounds(0L, indexSize);
		} else {
			return new IndexByteBounds(objectSize - (BYTES_PER_LONG * indexSize), objectSize - 1);
		}
	}

	private static class IndexByteBounds {

		private final long start;
		private final long end;

		private IndexByteBounds(long start, long end) {

			this.start = start;
			this.end = end;
		}
	}

	public static ShardIndex read(FileChannel channel, ShardedDatasetAttributes datasetAttributes) throws IOException {

		// TODO need codecs
		// TODO FileChannel is too specific - generalize
		final int[] indexShape = prepend(2, datasetAttributes.getShardBlockGridSize());
		final int indexSize = (int)Arrays.stream(indexShape).reduce(1, (x, y) -> x * y);
		final int indexBytes = BYTES_PER_LONG * indexSize;

		if (datasetAttributes.getIndexLocation() == IndexLocation.END) {
			channel.position(channel.size() - indexBytes);
		}

		final InputStream is = Channels.newInputStream(channel);
		final DataInputStream dis = new DataInputStream(is);

		final long[] indexes = new long[indexSize];
		for (int i = 0; i < indexSize; i++) {
			indexes[i] = dis.readLong();
		}

		return new ShardIndex(indexShape, indexes);
	}

	private static long[] emptyIndexData(final int[] size) {

		final int N = 2 * Arrays.stream(size).reduce(1, (x, y) -> x * y);
		final long[] data = new long[N];
		Arrays.fill(data, Shard.EMPTY_INDEX_NBYTES);
		return data;
	}

	private static int[] prepend(final int value, final int[] array) {

		final int[] indexBlockSize = new int[array.length + 1];
		indexBlockSize[0] = value;
		System.arraycopy(array, 0, indexBlockSize, 1, array.length);
		return indexBlockSize;
	}

	public static void main(String[] args) {

		final ShardIndex ib = new ShardIndex(new int[]{2, 2});

		ib.set(8, 9, new long[]{1, 1});

		// System.out.println(ib.getIndex(0, 0));
		// System.out.println(ib.getIndex(1, 0));
		// System.out.println(ib.getIndex(0, 1));
		// System.out.println(ib.getIndex(1, 1));

		System.out.println("done");
	}

}
