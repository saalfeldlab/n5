package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation;

public class RawShardStuff2 {


	public static void main(String[] args) {
		// DataBlocks are 3x3x3
		// Level 1 shards are 6x6x6 (contain 2x2x2 DataBlocks)
		// Level 2 shards are 24x24x24 (contain 4x4x4 Level 1 shards)
		final BlockCodecInfo c0 = new N5BlockCodecInfo();
		final ShardCodecInfo c1 = new DefaultShardCodecInfo(
				new int[] {3, 3, 3},
				c0,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.END
		);
		final ShardCodecInfo c2 = new DefaultShardCodecInfo(
				new int[] {6, 6, 6},
				c1,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.START
		);

		// TODO: This should go into DatasetAttributes.
		//       DatasetAccess<T> replaces DatasetAttributes.blockCodec
		//       DatasetAttributes.getDataAccess() replaces DatasetAttributes.getBlockCodec()
		//       All information required for ShardedDatasetAccess.create(...) is known to DatasetAttributes already.
		final DatasetAccess<byte[]> datasetAccess = ShardedDatasetAccess.create(DataType.INT8,
				new int[] {24, 24, 24},
				c2,
				new DataCodecInfo[] {new RawCompression()});

		// TODO: N5Reader/Writer needs to provide a PositionValueAccess implementation on top of its KVA.
		//       The read/write/deleteBlock methods would getDataAccess() from the DatasetAttributes and call it with that PositionValueAccess.
		final PositionValueAccess store = new TestPositionValueAccess();


		// ---------------------------------------------------------------
		// Some "tests"
		// TODO: Turn into unit tests
		// ---------------------------------------------------------------

		// write some blocks, filled with constant values
		final int[] dataBlockSize = c1.getInnerBlockSize();
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {0, 0, 0}, 1));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {1, 0, 0}, 2));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {0, 1, 0}, 3));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {1, 1, 0}, 4));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {3, 2, 1}, 5));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {8, 4, 1}, 6));

		// verify that the written blocks can be read back with the correct values
		checkBlock(datasetAccess.readBlock(store, new long[] {0, 0, 0}), true, 1);
		checkBlock(datasetAccess.readBlock(store, new long[] {1, 0, 0}), true, 2);
		checkBlock(datasetAccess.readBlock(store, new long[] {0, 1, 0}), true, 3);
		checkBlock(datasetAccess.readBlock(store, new long[] {1, 1, 0}), true, 4);
		checkBlock(datasetAccess.readBlock(store, new long[] {3, 2, 1}), true, 5);
		checkBlock(datasetAccess.readBlock(store, new long[] {8, 4, 1}), true, 6);

		// verify that deleting a block removes it from the shard (while other blocks in the same shard are still present)
		datasetAccess.deleteBlock(store, new long[] {0, 0, 0});
		checkBlock(datasetAccess.readBlock(store, new long[] {0, 0, 0}), false, 1);
		checkBlock(datasetAccess.readBlock(store, new long[] {1, 0, 0}), true, 2);

		// if a shard becomes empty the corresponding key should be deleted
		if ( store.get(new long[] {1, 0, 0}) == null ) {
			throw new IllegalStateException("expected non-null readData");
		}
		datasetAccess.deleteBlock(store, new long[] {8, 4, 1});
		if ( store.get(new long[] {1, 0, 0}) != null ) {
			throw new IllegalStateException("expected null readData");
		}

		// deleting a non-existent block should not fail
		datasetAccess.deleteBlock(store, new long[] {0, 0, 8});
	}

	private static void checkBlock(final DataBlock<byte[]> dataBlock, final boolean expectedNonNull, final int expectedFillValue) {

		if (dataBlock == null) {
			if (expectedNonNull) {
				throw new IllegalStateException("expected non-null dataBlock");
			}
		} else {
			if (!expectedNonNull) {
				throw new IllegalStateException("expected null dataBlock");
			}
			final byte[] bytes = dataBlock.getData();
			for (byte b : bytes) {
				if (b != (byte) expectedFillValue) {
					throw new IllegalStateException("expected all values to be " + expectedFillValue);
				}
			}
		}
	}

	private static DataBlock<byte[]> createDataBlock(int[] size, long[] gridPosition, int fillValue) {
		final byte[] bytes = new byte[DataBlock.getNumElements(size)];
		Arrays.fill(bytes, (byte) fillValue);
		return new ByteArrayDataBlock(size, gridPosition, bytes);
	}


	/**
	 * Dummy implementation of PositionValueAccess for testing. Just stores
	 * {@code byte[]} array values in a {@code Map} (instead of a {@code
	 * KeyValueAccess}.
	 */
	static class TestPositionValueAccess implements PositionValueAccess {

		private static class Key {

			private final long[] data;

			Key(long[] data) {
				this.data = data;
			}

			@Override
			public final boolean equals(final Object o) {
				if (!(o instanceof Key)) {
					return false;
				}
				final Key key = (Key) o;
				return Arrays.equals(data, key.data);
			}

			@Override
			public int hashCode() {
				return Arrays.hashCode(data);
			}
		}

		private final Map<Key, byte[]> map = new HashMap<>();

		@Override
		public ReadData get(final long[] key) {
			final byte[] bytes = map.get(new Key(key));
			return bytes == null ? null : ReadData.from(bytes);
		}

		@Override
		public void put(final long[] key, final ReadData data) {
			final byte[] bytes = data == null ? null : data.allBytes();
			map.put(new Key(key), bytes);
		}

		@Override
		public void remove(final long[] key) throws N5IOException {
			map.remove(new Key(key));
		}
	}

}
