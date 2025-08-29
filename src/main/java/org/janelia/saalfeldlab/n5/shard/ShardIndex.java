package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.IndexCodecAdapter;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * The ShardIndex tracks the offset and length of blocks contained within a
 * shard.
 * <p>
 * Blocks in a shard are arrayed in an n-dimensional grid, referred to as the
 * {@code shardBlockGrid}. The ShardIndex is implemented as an (n+1)-dimensional
 * {@link LongArrayDataBlock}, where the 0th dimensions is length 2 and contains
 * the block offsets and lengths. The grid position of the index iteself is meaningless,
 * and as a result, {@link #getGridPosition()} will return {@code null}.
 * <p>
 * The index stores two values for each block: offset and number of bytes. Blocks
 * that don't exist are marked with the special value {@link #EMPTY_INDEX_NBYTES}.
 * <p>
 * Block grid positions in this class are relative to the shard.
 *
 * @see <a href=
 *      "https://zarr-specs.readthedocs.io/en/latest/v3/codecs/sharding-indexed/index.html#binary-shard-format">The
 *      Zarr V3 specification for the binary shard format</a>
 */
public class ShardIndex extends LongArrayDataBlock {

	/**
	 * Special value indicating an empty block entry in the index.
	 * Used for both offset and length when a block doesn't exist.
	 */
	public static final long EMPTY_INDEX_NBYTES = 0xFFFFFFFFFFFFFFFFL;
	private static final int BYTES_PER_LONG = 8;
	private static final int LONGS_PER_BLOCK = 2;
	private static final long[] DUMMY_GRID_POSITION = null;

	private final IndexLocation location;
	private final ShardIndexAttributes indexAttributes;
	private final IndexCodecAdapter indexCodexAdapter;

	/**
	 * Creates a ShardIndex with specified data.
	 *
	 * @param shardBlockGridSize the dimensions of the block grid within the shard
	 * @param data the raw index data containing offsets and lengths
	 * @param location where the index is stored (START or END of shard)
	 * @param indexCodecAdapter data object for Shard Index codecs.
	 */
	public ShardIndex(int[] shardBlockGridSize, long[] data, IndexLocation location, final IndexCodecAdapter indexCodecAdapter) {

		// prepend the number of longs per block to the shard block grid size
		super(prepend(LONGS_PER_BLOCK, shardBlockGridSize), DUMMY_GRID_POSITION, data);
		this.indexCodexAdapter = indexCodecAdapter;
		this.location = location;
		this.indexAttributes = new ShardIndexAttributes(this);
	}

	/**
	 * Creates an empty ShardIndex at the specified location.
	 *
	 * @param shardBlockGridSize the dimensions of the block grid within the shard
	 * @param location where the index is stored (START or END of shard)
	 * @param indexCodecAdapter data object for idnex codecs
	 */
	public ShardIndex(int[] shardBlockGridSize, IndexLocation location, final IndexCodecAdapter indexCodecAdapter) {

		this(shardBlockGridSize, emptyIndexData(shardBlockGridSize), location, indexCodecAdapter);
	}

	/**
	 * Creates an empty ShardIndex at the default location (END).
	 *
	 * @param shardBlockGridSize the dimensions of the block grid within the shard
	 * @param indexCodecAdapter data object for idnex codecs
	 */
	public ShardIndex(int[] shardBlockGridSize, final IndexCodecAdapter indexCodecAdapter) {

		this(shardBlockGridSize, IndexLocation.END, indexCodecAdapter);
	}

	/**
	 * Creates an empty ShardIndex at the specified location.
	 *
	 * @param shardBlockGridSize the dimensions of the block grid within the shard
	 * @param location where the index is stored (START or END of shard)
	 * @param blockCodecInfo blockCodecInfo for the IndexCodecAdapter
	 */
	public ShardIndex(int[] shardBlockGridSize, IndexLocation location, final BlockCodecInfo blockCodecInfo) {

		this(shardBlockGridSize, location, new IndexCodecAdapter(blockCodecInfo));
	}

	/**
	 * Creates an empty ShardIndex at the default location (END).
	 *
	 * @param shardBlockGridSize the dimensions of the block grid within the shard
	 * @param blockCodecInfo blockCodecInfo for the IndexCodecAdapter
	 */
	public ShardIndex(int[] shardBlockGridSize, final BlockCodecInfo blockCodecInfo) {

		this(shardBlockGridSize, IndexLocation.END, blockCodecInfo);
	}

	public IndexCodecAdapter getIndexCodexAdapter() {

		return indexCodexAdapter;
	}

	/**
	 * Checks existence of the block at a given grid position.
	 *
	 * @param gridPosition the n-dimensional position of the block in the shard grid
	 * @return true if the block exists, false otherwise
	 */
	public boolean exists(int[] gridPosition) {

		return getOffset(gridPosition) != EMPTY_INDEX_NBYTES ||
				getNumBytes(gridPosition) != EMPTY_INDEX_NBYTES;
	}

	/**
	 * Checks existence of the block at a given flat index.
	 *
	 * @param index the flattened index of the block
	 * @return true if the block exists, false otherwise
	 */
	public boolean exists(int index) {

		return data[index * 2] != EMPTY_INDEX_NBYTES ||
				data[index * 2 + 1] != EMPTY_INDEX_NBYTES;
	}

	/**
	 * Gets the total number of blocks that can be stored in this index.
	 *
	 * @return the total number of blocks in the shard grid
	 */
	public int getNumBlocks() {

		/* getSize() is the number of data entries; each block takes 2 entries (offset and length)
		* so the product of the dimension sizes, divided by 2, is the number of blocks. */
		return Arrays.stream(getSize()).reduce(1, (x, y) -> x * y) / 2;
	}

	/**
	 * Checks if the index is completely empty (no blocks exist).
	 *
	 * @return true if no blocks exist in the index, false otherwise
	 */
	public boolean isEmpty() {

		return !IntStream.range(0, getNumBlocks()).anyMatch(this::exists);
	}

	/**
	 * Gets the location of this index within the shard.
	 *
	 * @return the index location (START or END)
	 */
	public IndexLocation getLocation() {

		return location;
	}

	/**
	 * Gets the offset in this shard in bytes for the block at a grid position.
	 *
	 * @param gridPosition the n-dimensional position of the block in the shard grid
	 * @return the offset in bytes, or {@link #EMPTY_INDEX_NBYTES} if the block doesn't exist
	 */
	public long getOffset(int... gridPosition) {

		return data[getOffsetIndex(gridPosition)];
	}

	/**
	 * Gets the offset in this shard in bytes for the block at a given index.
	 *
	 * @param index the flattened index of the block
	 * @return the offset in bytes, or {@link #EMPTY_INDEX_NBYTES} if the block doesn't exist
	 */
	public long getOffsetByBlockIndex(int index) {

		return data[index * 2];
	}

	/**
	 * Gets the number of bytes for the block at a grid position.
	 *
	 * @param gridPosition the n-dimensional position of the block in the shard grid
	 * @return the number of bytes, or {@link #EMPTY_INDEX_NBYTES} if the block doesn't exist
	 */
	public long getNumBytes(int... gridPosition) {

		return data[getNumBytesIndex(gridPosition)];
	}

	/**
	 * Gets the number of bytes for the block at a given index.
	 *
	 * @param index the flattened index of the block
	 * @return the number of bytes, or {@link #EMPTY_INDEX_NBYTES} if the block doesn't exist
	 */
	public long getNumBytesByBlockIndex(int index) {

		return data[index * 2 + 1];
	}

	/**
	 * Sets the offset and number of bytes for a block at the specified position.
	 *
	 * @param offset the byte offset of the block in the shard
	 * @param nbytes the number of bytes the block occupies
	 * @param gridPosition the n-dimensional position of the block in the shard grid
	 */
	public void set(long offset, long nbytes, int[] gridPosition) {

		final int i = getOffsetIndex(gridPosition);
		data[i] = offset;
		data[i + 1] = nbytes;
	}

	/**
	 * Marks a block position as empty.
	 *
	 * @param gridPosition the n-dimensional position of the block to mark as empty
	 */
	public void setEmpty(int[] gridPosition) {

		set(EMPTY_INDEX_NBYTES, EMPTY_INDEX_NBYTES, gridPosition);
	}

	/**
	 * Calculates the flattened array index for the offset value of a block.
	 *
	 * @param gridPosition the n-dimensional position of the block
	 * @return the index in the data array where the offset is stored
	 */
	protected int getOffsetIndex(int... gridPosition) {

		int idx = (int) gridPosition[0];
		int cumulativeSize = 1;
		for (int i = 1; i < gridPosition.length; i++) {
			cumulativeSize *= size[i];
			idx += gridPosition[i] * cumulativeSize;
		}
		return idx * 2;
	}

	/**
	 * Calculates the flattened array index for the number of bytes value of a block.
	 *
	 * @param gridPosition the n-dimensional position of the block
	 * @return the index in the data array where the number of bytes is stored
	 */
	protected int getNumBytesIndex(int... gridPosition) {

		return getOffsetIndex(gridPosition) + 1;
	}

	/**
	 * Calculates the total size of the index in bytes after compression.
	 *
	 * @return the total number of bytes the index occupies after applying all codecs
	 */
	public long numBytes() {

		final long numEntries = Arrays.stream(getSize()).reduce(1, (x, y) -> x * y);
		return getIndexCodexAdapter().encodedSize(numEntries * BYTES_PER_LONG);
	}

	/**
	 * Reads the index data from a shard.
	 *
	 * @param shardData the ReadData containing the entire shard
	 * @param index the ShardIndex to populate with data
	 * @throws N5IOException if the read operation fails or the shard data has invalid length
	 */
	public static void readFromShard(ReadData shardData, ShardIndex index) throws N5IOException {

		/* we require a length, so materialize if we don't have one. */
		if (shardData.length() == -1)
			shardData.materialize();

		final long length = shardData.length();
		if (length == -1)
			throw new N5IOException("ReadData for shard index must have a valid length, but was " + length);

		final long indexStartByte = ShardIndex.indexStartByte(index, length);
		final ReadData indexData = shardData.slice(indexStartByte, index.numBytes());
		ShardIndex.read(indexData, index);
	}

	/**
	 * Reads index data from a ReadData source.
	 *
	 * @param indexData the ReadData containing the index
	 * @param index the ShardIndex to populate with data
	 * @return true if the read was successful, false if the key doesn't exist
	 * @throws N5IOException if the read operation fails
	 */
	public static void read( final ReadData indexData, final ShardIndex index ) {

		final DataBlock<Object> decodedBlock = index.indexAttributes.getBlockCodec().decode(indexData, index.gridPosition);
		System.arraycopy(decodedBlock.getData(), 0, index.data, 0, index.data.length);
	}

	/**
	 * Reads index data from an InputStream.
	 *
	 * @param indexIn the InputStream containing the index data
	 * @param index the ShardIndex to populate with data
	 * @throws N5IOException if the read operation fails
	 */
	public static void read(InputStream indexIn, final ShardIndex index) throws N5IOException {

		final ReadData dataIn = ReadData.from(indexIn);
		final BlockCodec<long[]> shardIndexCodec = index.indexAttributes.getBlockCodec();
		final DataBlock<long[]> indexBlock = shardIndexCodec.decode(dataIn, index.gridPosition);
		System.arraycopy(indexBlock.getData(), 0, index.data, 0, (int)index.data.length);
	}

	/**
	 * Writes the index to an OutputStream.
	 *
	 * @param outputStream the OutputStream to write to
	 * @param index the ShardIndex to write
	 * @throws N5IOException if the write operation fails
	 */
	public static void write( final OutputStream outputStream, final ShardIndex index ) throws N5IOException {

		final BlockCodec<long[]> dataBlockSerializer = index.indexAttributes.getBlockCodec();
		dataBlockSerializer.encode(index).writeTo(outputStream);
	}

	/**
	 * DatasetAttributes for the ShardIndex, used for codec operations.
	 */
	private static class ShardIndexAttributes extends DatasetAttributes {

		/**
		 * Creates attributes for the given ShardIndex.
		 *
		 * @param index the ShardIndex
		 */
		public ShardIndexAttributes(ShardIndex index) {
			super(
					Arrays.stream(index.getSize()).mapToLong(it -> it).toArray(),
					index.getSize(),
					index.getSize(),
					DataType.UINT64,
					index.indexCodexAdapter.getBlockCodecInfo(),
					index.indexCodexAdapter.getDataCodecs()
					);
		}
	}

	/**
	 * Calculates the start byte of the index within a shard.
	 *
	 * @param index the ShardIndex
	 * @param objectSize the total size of the shard in bytes
	 * @return the start byte of the index
	 */
	public static long indexStartByte(final ShardIndex index, long objectSize) {

		return indexStartByte(index.numBytes(), index.location, objectSize);
	}

	/**
	 * Calculates the start byte an index within a shard.
	 *
	 * @param indexSize the size of the index in bytes
	 * @param indexLocation the location of the index (START or END)
	 * @param objectSize the total size of the shard in bytes
	 * @return the start byte of the index
	 */
	public static long indexStartByte(final long indexSize, final IndexLocation indexLocation, final long objectSize) {

		if (indexLocation == IndexLocation.START) {
			return 0L;
		} else {
			return objectSize - indexSize;
		}
	}

	/**
	 * Creates an empty index data array filled with {@link #EMPTY_INDEX_NBYTES}.
	 *
	 * @param size the dimensions of the block grid
	 * @return an array filled with empty values
	 */
	private static long[] emptyIndexData(final int[] size) {

		final int N = 2 * Arrays.stream(size).reduce(1, (x, y) -> x * y);
		final long[] data = new long[N];
		Arrays.fill(data, EMPTY_INDEX_NBYTES);
		return data;
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
}