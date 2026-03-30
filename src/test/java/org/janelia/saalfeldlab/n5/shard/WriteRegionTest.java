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

import java.util.Arrays;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;
import org.janelia.saalfeldlab.n5.N5Writer.DataBlockSupplier;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;
import org.junit.Test;

public class WriteRegionTest {


	@Test
	public void testWriteRegion() {

		int[] chunkSize = {3};
		final long[] datasetDimensions = {15};
		final BlockCodecInfo c0 = new N5BlockCodecInfo();

		TestDatasetAttributes attributes = new TestDatasetAttributes(
				datasetDimensions,
				chunkSize,
				DataType.INT8,
				c0,
				new RawCompression());

		final DatasetAccess<byte[]> datasetAccess = attributes.getDatasetAccess();
		final PositionValueAccess store = new TestPositionValueAccess();
		DataBlockSupplier<byte[]> chunks = (gridPos, existing) -> {
			return createDataBlock(chunkSize, gridPos.clone(), (byte) gridPos[0]);
		};
		
		DataBlockSupplier<byte[]> chunks255 = (gridPos, existing) -> {
			return createDataBlock(chunkSize, gridPos.clone(), (byte)255);
		};
		
		DataBlockSupplier<byte[]> chunksDelete = (gridPos, existing) -> {
			return null;
		};


		// write one chunk at grid Position 1
		datasetAccess.writeRegion(store,
				new long[] {3},
				new long[] {3},
				chunks,
				false);

		// Chunks
		//	|...|...|...|...|...|
		// Pixels indexes
		//	0   3   6   9   12   15-

		checkChunk(datasetAccess.readChunk(store, new long[] {0}), false, 0);
		checkChunk(datasetAccess.readChunk(store, new long[] {1}), true, 1);
		checkChunk(datasetAccess.readChunk(store, new long[] {2}), false, 0);
		checkChunk(datasetAccess.readChunk(store, new long[] {3}), false, 0);
		checkChunk(datasetAccess.readChunk(store, new long[] {4}), false, 0);
		
		// write two chunks at grid positions 2 and 3
		datasetAccess.writeRegion(store,
				new long[]{6},
				new long[]{6},
				chunks,
				false);


		checkChunk(datasetAccess.readChunk(store, new long[] {0}), false, 0);
		checkChunk(datasetAccess.readChunk(store, new long[] {1}), true, 1);
		checkChunk(datasetAccess.readChunk(store, new long[] {2}), true, 2);
		checkChunk(datasetAccess.readChunk(store, new long[] {3}), true, 3);
		checkChunk(datasetAccess.readChunk(store, new long[] {4}), false, 0);

		// delete two chunks at grid positions 3 and 4
		datasetAccess.writeRegion(store,
				new long[]{9},
				new long[]{6},
				chunksDelete,
				false);

	}

	@Test
	public void testWriteRegionSharded() {

		// Shards
		//	#...............................#...............................#...............................#...............................#
		// Chunks
		//	|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|
		// Pixels indexes
		//	0   3   6   9  12  15  18  21  24  27  30  33  36  39  42  45  48  51  54  57  60  63  66  69  72  75  78  81  84  87  90  93  96

		int[] chunkSize = {3};
		int[] shardSize = {24};
		final long[] datasetDimensions = {96};
		int numChunks = (int)(datasetDimensions[0] / chunkSize[0]);

		// Chunks are size 3
		// Shards are size 24 (contain 8 chunks)

		final BlockCodecInfo c0 = new N5BlockCodecInfo();
		final ShardCodecInfo c1 = new DefaultShardCodecInfo(
				chunkSize,
				c0,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.END
		);

		TestDatasetAttributes attributes = new TestDatasetAttributes(
				datasetDimensions,
				shardSize,
				DataType.INT8,
				c1,
				new RawCompression());

		final DatasetAccess<byte[]> datasetAccess = attributes.getDatasetAccess();
		final PositionValueAccess store = new TestPositionValueAccess();

		DataBlockSupplier<byte[]> chunks = (gridPos, existing) -> {
			return createDataBlock(chunkSize, gridPos.clone(), (byte) gridPos[0]);
		};

		DataBlockSupplier<byte[]> chunks255 = (gridPos, existing) -> {
			return createDataBlock(chunkSize, gridPos.clone(), (byte)255);
		};

		DataBlockSupplier<byte[]> chunksDelete = (gridPos, existing) -> {
			return null;
		};

		// write one chunk at grid Position 1
		datasetAccess.writeRegion(store,
				new long[] {3},
				new long[] {3},
				chunks,
				false);

		checkChunk(datasetAccess.readChunk(store, new long[] {0}), false, 0);
		checkChunk(datasetAccess.readChunk(store, new long[] {1}), true, 1);
		checkChunk(datasetAccess.readChunk(store, new long[] {2}), false, 0);
		checkChunk(datasetAccess.readChunk(store, new long[] {3}), false, 0);
		checkChunk(datasetAccess.readChunk(store, new long[] {4}), false, 0);

		// only the first shard should exist
		checkKey(store, new long[]{0}, true);
		checkKey(store, new long[]{1}, false);
		checkKey(store, new long[]{2}, false);
		checkKey(store, new long[]{3}, false);

		// write two chunks at grid positions 2 and 3
		datasetAccess.writeRegion(store,
				new long[]{6},
				new long[]{6},
				chunks,
				false);

		checkChunk(datasetAccess.readChunk(store, new long[]{0}), false, 0);
		checkChunk(datasetAccess.readChunk(store, new long[]{1}), true, 1);
		checkChunk(datasetAccess.readChunk(store, new long[]{2}), true, 2);
		checkChunk(datasetAccess.readChunk(store, new long[]{3}), true, 3);
		checkChunk(datasetAccess.readChunk(store, new long[]{4}), false, 0);

		// delete two chunks at grid positions 3 and 4
		datasetAccess.writeRegion(store,
				new long[]{9},
				new long[]{6},
				chunksDelete,
				false);

		checkChunk(datasetAccess.readChunk(store, new long[]{0}), false, 0);
		checkChunk(datasetAccess.readChunk(store, new long[]{1}), true, 1);
		checkChunk(datasetAccess.readChunk(store, new long[]{2}), true, 2);
		checkChunk(datasetAccess.readChunk(store, new long[]{3}), false, 0);
		checkChunk(datasetAccess.readChunk(store, new long[]{4}), false, 0);

		// overwrite all chunks to hold 255
		datasetAccess.writeRegion(store,
				new long[]{0},
				new long[]{96},
				chunks255,
				false);

		for (int i = 0; i < numChunks; i++) {
			checkChunk(datasetAccess.readChunk(store, new long[]{i}), true, 255);
		}

		// all shards should exist
		checkKey(store, new long[]{0}, true);
		checkKey(store, new long[]{1}, true);
		checkKey(store, new long[]{2}, true);
		checkKey(store, new long[]{3}, true);

		// delete some chunks
		datasetAccess.writeRegion(store,
				new long[]{18},
				new long[]{18},
				chunksDelete,
				false);

		checkChunk(datasetAccess.readChunk(store, new long[]{5}), true, 255);
		checkChunk(datasetAccess.readChunk(store, new long[]{6}), false, 0);

		// all shards should exist
		checkKey(store, new long[]{0}, true);
		checkKey(store, new long[]{1}, true);
		checkKey(store, new long[]{2}, true);
		checkKey(store, new long[]{3}, true);

		// delete more chunks so that shard 1 is empty
		datasetAccess.writeRegion(store,
				new long[]{36},
				new long[]{15},
				chunksDelete,
				false);

		// shard 1 should be gone
		checkKey(store, new long[]{0}, true);
		checkKey(store, new long[]{1}, false);
		checkKey(store, new long[]{2}, true);
		checkKey(store, new long[]{3}, true);
	}

	private static void checkChunk(final DataBlock<byte[]> chunk, final boolean expectedNonNull, final int expectedFillValue) {

		if (chunk == null) {
			if (expectedNonNull) {
				throw new IllegalStateException("expected non-null dataBlock");
			}
		} else {
			if (!expectedNonNull) {
				throw new IllegalStateException("expected null dataBlock");
			}
			final byte[] bytes = chunk.getData();
			for (byte b : bytes) {
				if (b != (byte) expectedFillValue) {
					throw new IllegalStateException("expected all values to be " + expectedFillValue);
				}
			}
		}
	}

	private static void checkKey(PositionValueAccess pva, long[] position, final boolean expectedNonNull) {

		try (VolatileReadData val = pva.get(position)) {

			if (expectedNonNull && val == null)
				throw new IllegalStateException("expected non-null value");
			else if (!expectedNonNull && val != null)
				throw new IllegalStateException("expected null value");
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

		@Override // to make this accessible for the test
		protected <T> DatasetAccess<T> getDatasetAccess() {
			return super.getDatasetAccess();
		}
	}

}
