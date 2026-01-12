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
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Writer.DataBlockSupplier;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;

public class WriteShardTest {


//	#...............................#...............................#...............................#...............................#
//	$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$
//	|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|
//	0   3   6   9  12  15  18  21  24  27  30  33  36  39  42  45  48  51  54  57  60  63  66  69  72  75  78  81  84  87  90  93  96

	public static void main(String[] args) {

		final int[] datablockSize = {3};
		final int[] level1ShardSize = {6};
		final long[] datasetDimensions = {36};

		// DataBlocks are 3
		// Level 1 shards are 6
		final BlockCodecInfo c0 = new N5BlockCodecInfo();
		final ShardCodecInfo c1 = new DefaultShardCodecInfo(
				datablockSize,
				c0,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.END
		);

		TestDatasetAttributes attributes = new TestDatasetAttributes(
				datasetDimensions,
				level1ShardSize,
				DataType.INT32,
				c1,
				new RawCompression());

		final DatasetAccess<int[]> datasetAccess = attributes.datasetAccess();
		final PositionValueAccess store = new TestPositionValueAccess();

		//	0       1       2       3       4       5       6
		//	$.......$.......$.......$.......$.......$.......$
		//	0   1   2   3   4   5   6   7   8   9  10  11  12
		//	|...|...|...|...|...|...|...|...|...|...|...|...|
		//	0   3   6   9  12  15  18  21  24  27  30  33  36
		//  .................................................

		final int[] dataBlockSize = c1.getInnerBlockSize();

		// create a shard DataBlock
		{
			final DataBlock<int[]> shard = createDataBlock(level1ShardSize, new long[] {2}, 1);
			System.out.println("shard.getGridPosition() = " + Arrays.toString(shard.getGridPosition()));
			System.out.println("shard.getSize() = " + Arrays.toString(shard.getSize()));
			System.out.println("shard.getData() = " + Arrays.toString(shard.getData()));
			System.out.println();
			datasetAccess.writeShard(store, shard, 1);
			System.out.println();
			System.out.println();
		}
		{
			final DataBlock<int[]> shard = createDataBlock(level1ShardSize, new long[] {5}, 1);
			System.out.println("shard.getGridPosition() = " + Arrays.toString(shard.getGridPosition()));
			System.out.println("shard.getSize() = " + Arrays.toString(shard.getSize()));
			System.out.println("shard.getData() = " + Arrays.toString(shard.getData()));
			System.out.println();
			datasetAccess.writeShard(store, shard, 1);
			System.out.println();
			System.out.println();
		}



		System.out.println("all good");
	}

	private static DataBlock<int[]> createDataBlock(int[] size, long[] gridPosition, int startValue) {
		final int[] ints = new int[DataBlock.getNumElements(size)];
		Arrays.setAll(ints, i -> i + startValue);
		return new IntArrayDataBlock(size, gridPosition, ints);
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
