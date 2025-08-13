package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.ArrayCodec;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.DataBlockSerializer;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.IndexCodecAdapter;
import org.janelia.saalfeldlab.n5.codec.RawBytesArrayCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Serialize(false)
public class BlockAsShardCodec extends ShardingCodec {

	private  static final LongArrayDataBlock BLOCK_AS_SHARD_INDEX = new LongArrayDataBlock(new int[0], new long[0], new long[]{0, -1});
	private static final DataBlockSerializer<long[]> BLOCK_AS_SHARD_BLOCK_SERIALIZER = new DataBlockSerializer<long[]>() {

		@Override public ReadData encode(DataBlock<long[]> dataBlock) throws N5Exception.N5IOException {

			return ReadData.from(new byte[0]);
		}

		@Override public DataBlock<long[]> decode(ReadData readData, long[] gridPosition) throws N5Exception.N5IOException {

			return BLOCK_AS_SHARD_INDEX;
		}
	};

	private static final RawBytesArrayCodec VIRTUAL_SHARD_INDEX_CODEC = new RawBytesArrayCodec() {

		@Override public DataBlockSerializer<long[]> initialize(DatasetAttributes attributes, BytesCodec... bytesCodecs) {

			return BLOCK_AS_SHARD_BLOCK_SERIALIZER;
		}
	};
	private static final BytesCodec[] EMPTY_SHARD_CODECS = new BytesCodec[0];

	private static final DeterministicSizeCodec[] NO_OP_INDEX_CODECS = new DeterministicSizeCodec[0];
	private static final IndexCodecAdapter BLOCK_AS_SHARD_INDEX_CODEC_ADAPTER = new IndexCodecAdapter(VIRTUAL_SHARD_INDEX_CODEC, NO_OP_INDEX_CODECS) {

		@Override public long encodedSize(long initialSize) {

			return 0;
		}
	};

	final ArrayCodec datasetArrayCodec;

	public BlockAsShardCodec(ArrayCodec datasetArrayCodec) {

		super(null, EMPTY_SHARD_CODECS, BLOCK_AS_SHARD_INDEX_CODEC_ADAPTER, IndexLocation.START);
		this.datasetArrayCodec = datasetArrayCodec;
	}

	@Override
	public ShardIndex createIndex(DatasetAttributes attributes) {

		return new ShardIndex(attributes.getBlocksPerShard(), getIndexLocation(), BLOCK_AS_SHARD_INDEX_CODEC_ADAPTER);
	}

	@Override
	public ArrayCodec getArrayCodec() {

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
	public <T> DataBlockSerializer<T> initialize(DatasetAttributes attributes, BytesCodec... codecs) {

		dataBlockSerializer = datasetArrayCodec.initialize(attributes, codecs);
		return (DataBlockSerializer<T>)dataBlockSerializer;
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
