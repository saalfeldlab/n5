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
import java.util.List;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class DatasetAccessTest {

	// DataBlocks are 3x3x3
	// Level 1 shards are 6x6x6 (contain 2x2x2 DataBlocks)
	// Level 2 shards are 24x24x24 (contain 4x4x4 Level 1 shards)
	private final int[] dataBlockSize = {3, 3, 3};
	private final int[] level1ShardSize = {6, 6, 6};
	private final int[] level2ShardSize = {24, 24, 24};
	private final long[] datasetDimensions = {240, 240, 240};

	private DatasetAccess<byte[]> datasetAccess;

	@Before
	public void setup() {

		final BlockCodecInfo c0 = new N5BlockCodecInfo();
		final ShardCodecInfo c1 = new DefaultShardCodecInfo(
				dataBlockSize,
				c0,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.END
		);
		final ShardCodecInfo c2 = new DefaultShardCodecInfo(
				level1ShardSize,
				c1,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.START
		);
		final TestDatasetAttributes attributes = new TestDatasetAttributes(
				datasetDimensions,
				level2ShardSize,
				DataType.INT8,
				c2,
				new RawCompression()
		);

		datasetAccess = attributes.datasetAccess();
	}


	@Test
	public void testWriteReadIndividual() {

		final PositionValueAccess store = new TestPositionValueAccess();

		// write some blocks, filled with constant values
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
	}

	@Test
	public void testWriteReadBulk() {

		final PositionValueAccess store = new TestPositionValueAccess();

		// write some blocks, filled with constant values
		final List<long[]> writeGridPositions = Arrays.asList(new long[][] {
				{0, 0, 0}, {1, 0, 0}, {0, 1, 0}, {1, 1, 0}, {3, 2, 1}, {8, 4, 1}
		});
		final List<DataBlock<byte[]>> writeBlocks = new ArrayList<>();
		for (int i = 0; i < writeGridPositions.size(); i++) {
			writeBlocks.add(createDataBlock(dataBlockSize, writeGridPositions.get(i), 1 + i));
		}
		datasetAccess.writeBlocks(store, writeBlocks);

		// verify that the written blocks can be read back with the correct values
		final List<long[]> readGridPositions = Arrays.asList(new long[][] {
				{1, 0, 0}, {0, 0, 0}, {0, 1, 0}, {2, 4, 2}, {3, 2, 1}, {8, 4, 1}
		});
		final List<DataBlock<byte[]>> readBlocks = datasetAccess.readBlocks(store, readGridPositions);
		checkBlock(readBlocks.get(0), true, 2);
		checkBlock(readBlocks.get(1), true, 1);
		checkBlock(readBlocks.get(2), true, 3);
		checkBlock(readBlocks.get(3), false, 4);
		checkBlock(readBlocks.get(4), true, 5);
		checkBlock(readBlocks.get(5), true, 6);
	}

	@Test
	public void testDeleteBlock() {

		final PositionValueAccess store = new TestPositionValueAccess();

		// write some blocks, filled with constant values
		final List<long[]> writeGridPositions = Arrays.asList(new long[][] {
				{0, 0, 0}, {1, 0, 0}, {0, 1, 0}, {1, 1, 0}, {3, 2, 1}, {8, 4, 1}
		});
		final List<DataBlock<byte[]>> writeBlocks = new ArrayList<>();
		for (int i = 0; i < writeGridPositions.size(); i++) {
			writeBlocks.add(createDataBlock(dataBlockSize, writeGridPositions.get(i), 1 + i));
		}
		datasetAccess.writeBlocks(store, writeBlocks);

		// verify that deleting a block removes it from the shard (while other blocks in the same shard are still present)
		datasetAccess.deleteBlock(store, new long[] {0, 0, 0});
		checkBlock(datasetAccess.readBlock(store, new long[] {0, 0, 0}), false, 1);
		checkBlock(datasetAccess.readBlock(store, new long[] {1, 0, 0}), true, 2);

		// if a shard becomes empty the corresponding key should be deleted
		assertTrue(keyExists(store, new long[] {1, 0, 0}));
		datasetAccess.deleteBlock(store, new long[] {8, 4, 1});
		assertFalse(keyExists(store, new long[] {1, 0, 0}));

		// deleting a non-existent block should not fail
		datasetAccess.deleteBlock(store, new long[] {0, 0, 8});
	}

	private boolean keyExists(final PositionValueAccess store, final long[] key) {
		try (final VolatileReadData data = store.get(key)) {
			if (data != null) {
				data.requireLength();
				return true;
			}
		} catch (N5Exception.N5IOException ignored) {
		}
		return false;
	}

	@Test
	public void testDeleteBlocks() {

		final PositionValueAccess store = new TestPositionValueAccess();

		// write some blocks, filled with constant values
		final List<long[]> writeGridPositions = Arrays.asList(new long[][] {
				{0, 0, 0}, {1, 0, 0}, {0, 1, 0}, {1, 1, 0}, {3, 2, 1}, {8, 4, 1}
		});
		final List<DataBlock<byte[]>> writeBlocks = new ArrayList<>();
		for (int i = 0; i < writeGridPositions.size(); i++) {
			writeBlocks.add(createDataBlock(dataBlockSize, writeGridPositions.get(i), 1 + i));
		}
		datasetAccess.writeBlocks(store, writeBlocks);

		// verify that deleting a block removes it from the shard (while other blocks in the same shard are still present)
		datasetAccess.deleteBlocks(store, Arrays.asList(new long[][] {{0, 0, 0}, {4, 2, 2}, {3, 2, 1}}));
		checkBlock(datasetAccess.readBlock(store, new long[] {0, 0, 0}), false, 1);
		checkBlock(datasetAccess.readBlock(store, new long[] {1, 0, 0}), true, 2);

		// if a shard becomes empty the corresponding key should be deleted
		assertTrue(keyExists(store, new long[] {1, 0, 0}));
		datasetAccess.deleteBlocks(store, Arrays.asList(new long[][] {{8, 4, 1}}));
		assertFalse(keyExists(store, new long[] {1, 0, 0}));

		// deleting a non-existent block should not fail
		datasetAccess.deleteBlocks(store, Arrays.asList(new long[] {0, 0, 8}));
	}

	private static void checkBlock(final DataBlock<byte[]> dataBlock, final boolean expectedNonNull, final int expectedFillValue) {

		if (expectedNonNull) {
			assertNotNull("expected non-null dataBlock", dataBlock);
			for (byte b : dataBlock.getData()) {
				Assert.assertTrue("expected all values to be " + expectedFillValue, b == (byte) expectedFillValue);
			}
		} else {
			assertNull("expected null dataBlock", dataBlock);
		}
	}

	private static DataBlock<byte[]> createDataBlock(int[] size, long[] gridPosition, int fillValue) {

		final byte[] bytes = new byte[DataBlock.getNumElements(size)];
		Arrays.fill(bytes, (byte) fillValue);
		return new ByteArrayDataBlock(size, gridPosition, bytes);
	}

	public static class TestDatasetAttributes extends DatasetAttributes {

		public TestDatasetAttributes(long[] dimensions, int[] outerBlockSize, DataType dataType, BlockCodecInfo blockCodecInfo,
				DataCodecInfo... dataCodecInfos) {

			super(dimensions, outerBlockSize, dataType, blockCodecInfo, dataCodecInfos);
		}

		public DatasetAccess datasetAccess() {

			// to make this accessible for the test
			return createDatasetAccess();
		}

	}

}
