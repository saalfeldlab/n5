package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodecInfo;
import org.janelia.saalfeldlab.n5.codec.IndexCodecAdapter;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Serialize(false)
public class BlockAsShardCodec extends ShardingCodec {

	private  static final LongArrayDataBlock BLOCK_AS_SHARD_INDEX = new LongArrayDataBlock(new int[0], new long[0], new long[]{0, -1});
	private static final BlockCodec<long[]> BLOCK_AS_SHARD_BLOCK_SERIALIZER = new BlockCodec<long[]>() {

		@Override public ReadData encode(DataBlock<long[]> dataBlock) throws N5Exception.N5IOException {

			return ReadData.from(new byte[0]);
		}

		@Override public DataBlock<long[]> decode(ReadData readData, long[] gridPosition) throws N5Exception.N5IOException {

			return BLOCK_AS_SHARD_INDEX;
		}
	};

	private static final RawBlockCodecInfo VIRTUAL_SHARD_INDEX_CODEC = new RawBlockCodecInfo() {

		@Override public BlockCodec<long[]> create(DatasetAttributes attributes, DataCodec... dataCodec) {

			return BLOCK_AS_SHARD_BLOCK_SERIALIZER;
		}
	};
	private static final DataCodecInfo[] EMPTY_SHARD_CODECS = new DataCodecInfo[0];

	private static final DeterministicSizeCodecInfo[] NO_OP_INDEX_CODECS = new DeterministicSizeCodecInfo[0];
	private static final IndexCodecAdapter BLOCK_AS_SHARD_INDEX_CODEC_ADAPTER = new IndexCodecAdapter(VIRTUAL_SHARD_INDEX_CODEC, NO_OP_INDEX_CODECS) {

		@Override public long encodedSize(long initialSize) {

			return 0;
		}
	};

	final BlockCodecInfo datasetArrayCodec;

	public BlockAsShardCodec(BlockCodecInfo datasetArrayCodec) {

		super(null, EMPTY_SHARD_CODECS, BLOCK_AS_SHARD_INDEX_CODEC_ADAPTER, IndexLocation.START);
		this.datasetArrayCodec = datasetArrayCodec;
	}

	@Override
	public ShardIndex createIndex(DatasetAttributes attributes) {

		return new ShardIndex(attributes.getBlocksPerShard(), getIndexLocation(), BLOCK_AS_SHARD_INDEX_CODEC_ADAPTER);
	}

	@Override
	public BlockCodecInfo getBlockCodecInfo() {

		return datasetArrayCodec;
	}

	@Override
	public long[] getKeyPositionForBlock(DatasetAttributes attributes, DataBlock<?> datablock) {

		return datasetArrayCodec.getKeyPositionForBlock(attributes, datablock);
	}

	@Override
	public long[] getKeyPositionForBlock(DatasetAttributes attributes, long... blockPosition) {

		return datasetArrayCodec.getKeyPositionForBlock(attributes, blockPosition);
	}

	@Override
	public <T> BlockCodec<T> create(DatasetAttributes attributes, DataCodec... codecs) {

		dataBlockSerializer = datasetArrayCodec.create(attributes, codecs);
		return (BlockCodec<T>)dataBlockSerializer;
	}

	@Override
	public long encodedSize(long size) {

		return datasetArrayCodec.encodedSize(size);
	}

	@Override
	public long decodedSize(long size) {

		return datasetArrayCodec.decodedSize(size);
	}

	@Override
	public String getType() {
		//TODO Caleb: can we ensure this is never called?
		return datasetArrayCodec.getType();
	}
}
