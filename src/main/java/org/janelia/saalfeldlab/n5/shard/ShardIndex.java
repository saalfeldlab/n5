package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class ShardIndex extends LongArrayDataBlock {

	private static final int BYTES_PER_LONG = 8;

	private static final int LONGS_PER_BLOCK = 2;

	private static final long[] DUMMY_GRID_POSITION = null;
	private final IndexLocation location;

	private final DeterministicSizeCodec[] codecs;

	public ShardIndex(int[] shardBlockGridSize, long[] data, IndexLocation location, final DeterministicSizeCodec... codecs) {

		super(prepend(LONGS_PER_BLOCK, shardBlockGridSize), DUMMY_GRID_POSITION, data);
		this.codecs = codecs;
		this.location = location;
	}

	public ShardIndex(int[] shardBlockGridSize, IndexLocation location, DeterministicSizeCodec... codecs) {

		this(shardBlockGridSize, emptyIndexData(shardBlockGridSize), location, codecs);
	}

	public ShardIndex(int[] shardBlockGridSize, DeterministicSizeCodec... codecs) {

		this(shardBlockGridSize, emptyIndexData(shardBlockGridSize), IndexLocation.END, codecs);
	}

	public boolean exists(long... gridPosition) {

		return getOffset(gridPosition) != Shard.EMPTY_INDEX_NBYTES &&
				getNumBytes(gridPosition) != Shard.EMPTY_INDEX_NBYTES;
	}

	public IndexLocation getLocation() {

		return location;
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

		int idx = (int) gridPosition[0];
		for (int i = 1; i < gridPosition.length; i++) {
			idx += gridPosition[i] * size[i];
		}
		return idx * 2;
	}

	private int getNumBytesIndex(long... gridPosition) {

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

	public static ShardIndex read(
			final KeyValueAccess keyValueAccess,
			final String key,
			final ShardIndex index
	) throws IOException {

		final IndexByteBounds byteBounds = byteBounds(index, keyValueAccess.size(key));
		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(key, byteBounds.start, byteBounds.end)) {
			final long[] indexData;
			try (final InputStream in = lockedChannel.newInputStream()) {
				final DataBlock<long[]> indexBlock = (DataBlock<long[]>)DefaultBlockReader.readBlock(
						in,
						index.getIndexAttributes(),
						index.gridPosition);
				indexData = indexBlock.getData();
			}
			System.arraycopy(indexData, 0, index.data, 0, index.data.length);
			return index;
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read shard index from " + key, e);
		}
	}

	public static void write(
			final ShardIndex index,
			final KeyValueAccess keyValueAccess,
			final String key
	) throws IOException {

		final long start = index.location == IndexLocation.START ? 0 : keyValueAccess.size(key);
		try (final LockedChannel lockedChannel = keyValueAccess.lockForWriting(key, start, index.numBytes())) {
			try (final OutputStream os = lockedChannel.newOutputStream()) {
				write(index, os);
			}
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to write shard index to " + key, e);
		}
	}

	public static void write(final ShardIndex index, OutputStream out) throws IOException {

		DefaultBlockWriter.writeBlock(out, index.getIndexAttributes(), index);
	}

	private DatasetAttributes getIndexAttributes() {

		final DatasetAttributes indexAttributes =
				new DatasetAttributes(
						Arrays.stream(getSize()).mapToLong(it -> it).toArray(),
						getSize(),
						DataType.UINT64,
						null,
						codecs
				);
		return indexAttributes;
	}

	public static IndexByteBounds byteBounds(ShardedDatasetAttributes datasetAttributes, final long objectSize) {

		final long indexSize = datasetAttributes.createIndex().numBytes();
		return byteBounds(indexSize, datasetAttributes.getIndexLocation(), objectSize);
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
		final int[] indexShape = prepend(2, datasetAttributes.getBlocksPerShard());
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

		return new ShardIndex(indexShape, indexes, IndexLocation.END);
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
