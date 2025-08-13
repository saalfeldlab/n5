package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name(value = N5ArrayCodec.TYPE)
public class N5ArrayCodec implements ArrayCodec {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String TYPE = "n5bytes";

	private transient DatasetAttributes attributes;

	@Override public <T> DataBlockSerializer<T> initialize(final DatasetAttributes attributes, final BytesCodec... byteCodecs) {
		this.attributes = attributes;
		return N5DataBlockSerializers.create(attributes.getDataType(), new ConcatenatedBytesCodec(byteCodecs));
	}

	@Override public long[] getKeyPositionForBlock(DatasetAttributes attributes, DataBlock<?> datablock) {

		return datablock.getGridPosition();
	}

	@Override public long[] getKeyPositionForBlock(DatasetAttributes attributes, long... blockPosition) {

		return blockPosition;
	}

	@Override public long encodedSize(long size) {

		final int[] blockSize = attributes.getBlockSize();
		int headerSize = new N5DataBlockSerializers.BlockHeader(blockSize, DataBlock.getNumElements(blockSize)).getSize();
		return headerSize + size;
	}

	@Override
	public String getType() {

		return TYPE;
	}
}
