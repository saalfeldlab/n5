package org.janelia.saalfeldlab.n5;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.Codec.ArrayCodec;
import org.janelia.saalfeldlab.n5.codec.Codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.shard.ShardIndex;
import org.janelia.saalfeldlab.n5.shard.ShardParameters;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;

public class ShardedDatasetAttributes extends DatasetAttributes implements ShardParameters {

	private static final long serialVersionUID = -4559068841006651814L;

	private final int[] shardSize;

	private final ShardingCodec shardingCodec;

	public ShardedDatasetAttributes (
			final long[] dimensions,
			final int[] shardSize, //in pixels
			final int[] blockSize, //in pixels
			final DataType dataType,
			final Codec[] blocksCodecs,
			final DeterministicSizeCodec[] indexCodecs,
			final IndexLocation indexLocation
	) {
		super(dimensions, blockSize, dataType, null, blocksCodecs);

		if (!validateShardBlockSize(shardSize, blockSize)) {
			throw new N5Exception(String.format("Invalid shard %s / block size %s",
					Arrays.toString(shardSize),
					Arrays.toString(blockSize)));
		}

		this.shardSize = shardSize;
		this.shardingCodec = new ShardingCodec(
				blockSize,
				blocksCodecs,
				indexCodecs,
				indexLocation
		);
	}

	public ShardedDatasetAttributes(
			final long[] dimensions,
			final int[] shardSize, //in pixels
			final int[] blockSize, //in pixels
			final DataType dataType,
			final ShardingCodec codec) {
		super(dimensions, blockSize, dataType, null, null);
		this.shardSize = shardSize;
		this.shardingCodec = codec;
	}

	/**
	 * Returns whether the given shard and block sizes are valid. Specifically, is
	 * the shard size a multiple of the block size in every dimension.
	 *
	 * @param shardSize size of the shard in pixels
	 * @param blockSize size of a block in pixels
	 * @return
	 */
	public static boolean validateShardBlockSize(final int[] shardSize, final int[] blockSize) {

		if (shardSize.length != blockSize.length)
			return false;

		for (int i = 0; i < shardSize.length; i++) {
			if (shardSize[i] % blockSize[i] != 0)
				return false;
		}
		return true;
	}

	public ShardingCodec getShardingCodec() {
		return shardingCodec;
	}

	@Override public ArrayCodec getArrayCodec() {

		return shardingCodec.getArrayCodec();
	}

	@Override public BytesCodec[] getCodecs() {

		return shardingCodec.getCodecs();
	}

	@Override
	protected Codec[] concatenateCodecs() {

		return new Codec[] { shardingCodec };
	}

	@Override
	public IndexLocation getIndexLocation() {

		return getShardingCodec().getIndexLocation();
	}

	@Override
	public ShardIndex createIndex() {
		return new ShardIndex(getBlocksPerShard(), getIndexLocation(), getShardingCodec().getIndexCodecs());
	}

	/**
	 * The size of the blocks in pixel units.
	 *
	 * @return the number of pixels per dimension for this shard.
	 */
	@Override
	public int[] getShardSize() {

		return shardSize;
	}

	public static int[] getBlockSize(Codec[] codecs) {

		for (final Codec codec : codecs)
			if (codec instanceof ShardingCodec)
				return ((ShardingCodec)codec).getBlockSize();

		return null;
	}
}
