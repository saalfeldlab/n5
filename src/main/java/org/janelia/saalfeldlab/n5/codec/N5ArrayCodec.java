package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name(value = N5ArrayCodec.TYPE)
public class N5ArrayCodec implements ArrayCodec {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String TYPE = "n5bytes";

	private transient BytesCodec bytesCodec;

	private transient DatasetAttributes attributes;

	@Override
	public void initialize(final DatasetAttributes attributes, final BytesCodec[] bytesCodecs) {
		this.attributes = attributes;
		this.bytesCodec = BytesCodec.concatenate(bytesCodecs);
	}

	private <T> DataBlockSerializer<T> getDataBlockCodec() {
		return N5DataBlockSerializers.createDataBlockCodec(attributes.getDataType(), bytesCodec);
	}

	@Override
	public <T> DataBlock<T> decode(ReadData readData, long[] gridPosition) {

		// TODO (TP): It is not good to create a new DataBlockCodec for every block.
		return this.<T>getDataBlockCodec().decode(readData, gridPosition);
	}

	@Override
	public <T> ReadData encode(DataBlock<T> dataBlock) {

		// TODO (TP): It is not good to create a new DataBlockCodec for every block.
		return this.<T>getDataBlockCodec().encode(dataBlock);
	}

	@Override
	public long encodedSize(long size) {

		final int[] blockSize = attributes.getBlockSize();
		int headerSize = new N5DataBlockSerializers.BlockHeader(blockSize, DataBlock.getNumElements(blockSize)).getSize();
		return headerSize + size;
	}

	@Override
	public String getType() {

		return TYPE;
	}
}
