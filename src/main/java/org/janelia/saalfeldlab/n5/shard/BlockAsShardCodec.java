package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.RawBytes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Serialize(false)
public class BlockAsShardCodec extends ShardingCodec {

	private static final RawBytes VIRTUAL_SHARD_INDEX_CODEC = new RawBytes() {

		@Override
		public ReadData encode(DataBlock dataBlock) throws N5Exception.N5IOException {

			return ReadData.from(new byte[0]);
		}

		@Override
		public DataBlock decode(ReadData readData, long[] gridPosition) throws N5Exception.N5IOException {

			final long[] data = new long[]{ 0, -1};
			return new LongArrayDataBlock(new int[0], new long[0], data);
		}
	};

	private static final BytesCodec[] EMPTY_SHARD_CODECS = new BytesCodec[0];
	private static final DeterministicSizeCodec[] NO_OP_INDEX_CODECS = new DeterministicSizeCodec[]{VIRTUAL_SHARD_INDEX_CODEC};

	final ArrayCodec datasetArrayCodec;
	private DatasetAttributes datasetAttributes;

	public BlockAsShardCodec(ArrayCodec datasetArrayCodec) {

		super(null, EMPTY_SHARD_CODECS, NO_OP_INDEX_CODECS, IndexLocation.START);
		this.datasetArrayCodec = datasetArrayCodec;
	}

	@Override
	public ShardIndex createIndex(DatasetAttributes attributes) {

		return new ShardIndex(attributes.getBlocksPerShard(), getIndexLocation(), NO_OP_INDEX_CODECS) {

			@Override public long numBytes() {

				return 0;
			}
		};
	}

	@Override
	public int[] getBlockSize() {

		return datasetAttributes.getBlockSize();
	}

	@Override
	public ArrayCodec getArrayCodec() {

		return datasetArrayCodec;
	}

	@Override
	public long[] getPositionForBlock(DatasetAttributes attributes, DataBlock<?> datablock) {

		return datasetArrayCodec.getPositionForBlock(attributes, datablock);
	}

	@Override
	public long[] getPositionForBlock(DatasetAttributes attributes, long... blockPosition) {

		return datasetArrayCodec.getPositionForBlock(attributes, blockPosition);
	}

	@Override
	public void initialize(DatasetAttributes attributes, BytesCodec... codecs) {

		this.datasetAttributes = attributes;
		datasetArrayCodec.initialize(attributes, codecs);
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

	@Override
	public <T> ReadData encode(DataBlock<T> dataBlock) throws N5Exception.N5IOException {

		return datasetArrayCodec.encode(dataBlock);
	}

	@Override
	public <T> DataBlock<T> decode(ReadData readData, long[] gridPosition) throws N5Exception.N5IOException {

		return datasetArrayCodec.decode(readData, gridPosition);
	}
}
