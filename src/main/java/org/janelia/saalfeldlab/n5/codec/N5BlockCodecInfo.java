package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name(value = N5BlockCodecInfo.TYPE)
public class N5BlockCodecInfo implements BlockCodecInfo {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String TYPE = "n5bytes";

	private transient DatasetAttributes attributes;

	@Override public long[] getKeyPositionForBlock(DatasetAttributes attributes, DataBlock<?> datablock) {

		return datablock.getGridPosition();
	}

	@Override public long[] getKeyPositionForBlock(DatasetAttributes attributes, long... blockPosition) {

		return blockPosition;
	}

	@Override public long encodedSize(long size) {

		final int[] blockSize = attributes.getBlockSize();
		int headerSize = new N5BlockCodecs.BlockHeader(blockSize, DataBlock.getNumElements(blockSize)).getSize();
		return headerSize + size;
	}

	@Override
	public String getType() {

		return TYPE;
	}

	@Override
	public <T> BlockCodec<T> create(final DatasetAttributes attributes, final DataCodec... dataCodecs) {
		this.attributes = attributes;
		return N5BlockCodecs.create(attributes.getDataType(), DataCodec.concatenate(dataCodecs));
	}

}
