package org.janelia.saalfeldlab.n5.shard;

import org.apache.commons.io.input.BoundedInputStream;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.stream.IntStream;

public class ShardIndex extends LongArrayDataBlock {

	public static final long EMPTY_INDEX_NBYTES = 0xFFFFFFFFFFFFFFFFL;
	private static final int BYTES_PER_LONG = 8;
	private static final int LONGS_PER_BLOCK = 2;
	private static final long[] DUMMY_GRID_POSITION = null;

	private final IndexLocation location;

	private final DeterministicSizeCodec[] codecs;
	private final DatasetAttributes indexAttributes;

	public ShardIndex(int[] shardBlockGridSize, long[] data, IndexLocation location, final DeterministicSizeCodec... codecs) {

		super(prepend(LONGS_PER_BLOCK, shardBlockGridSize), DUMMY_GRID_POSITION, data);
		this.codecs = codecs;
		this.location = location;
		this.indexAttributes = createIndexAttributes();
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

	public static boolean read(byte[] data, final ShardIndex index) {

		final IndexByteBounds byteBounds = byteBounds(index, data.length);
		final ByteArrayInputStream is = new ByteArrayInputStream(data);
		is.skip(byteBounds.start);

		try {
			BoundedInputStream bIs = BoundedInputStream.builder()
					.setInputStream(is)
					.setMaxCount(index.numBytes()).get();
			read(bIs, index);
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	public static void read(InputStream in, final ShardIndex index) throws IOException {

		@SuppressWarnings("unchecked") final DataBlock<long[]> indexBlock = (DataBlock<long[]>)DefaultBlockReader.readBlock(in, index.createIndexAttributes(), index.gridPosition);
		final long[] indexData = indexBlock.getData();
		System.arraycopy(indexData, 0, index.data, 0, index.data.length);
	}

	public static boolean read(
			final ReadData indexData,
			final ShardIndex index
	) {

		try (final InputStream in = indexData.inputStream()) {
			read(in, index);
			return true;
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return false;
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read shard index", e);
		}
	}

	public static void write(
			final OutputStream outputStream,
			final ShardIndex index
	) throws IOException {

		try {
			write(index, outputStream);
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to write shard index", e);
		}
	}

	public static void write(final ShardIndex index, OutputStream out) throws IOException {

		DefaultBlockWriter.writeBlock(out, index.createIndexAttributes(), index);
	}

	public Codec.ArrayCodec<?> getArrayCodec() {
		return indexAttributes.getArrayCodec();
	}

	// TODO: Caleb static?
	private DatasetAttributes createIndexAttributes() {

		final DatasetAttributes indexAttributes =
				new DatasetAttributes(
						Arrays.stream(getSize()).mapToLong(it -> it).toArray(),
						getSize(),
						DataType.UINT64,
						codecs
				);
		return indexAttributes;
	}

	public static IndexByteBounds byteBounds(DatasetAttributes datasetAttributes, final long objectSize) {

		ShardingCodec shardCodec = (ShardingCodec)datasetAttributes.getArrayCodec();
		final ShardIndex index = shardCodec.createIndex(datasetAttributes);

		final long indexSize = index.numBytes();
		return byteBounds(indexSize, index.location, objectSize);
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

	//TODO Caleb: Probably don't need to keep this eventually
	public static ShardIndex read(FileChannel channel, DatasetAttributes datasetAttributes) throws IOException {

		// TODO need codecs
		// TODO FileChannel is too specific - generalize
		ShardingCodec shardingCodec = (ShardingCodec)datasetAttributes.getArrayCodec();
		final int[] indexShape = prepend(2, datasetAttributes.getBlocksPerShard());
		final int indexSize = (int)Arrays.stream(indexShape).reduce(1, (x, y) -> x * y);
		final int indexBytes = BYTES_PER_LONG * indexSize;

		if (shardingCodec.getIndexLocation() == IndexLocation.END) {
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

		ShardingCodec shardingCodec = (ShardingCodec)attributes.getArrayCodec();
		return shardingCodec.createIndex(attributes);
	}
}

