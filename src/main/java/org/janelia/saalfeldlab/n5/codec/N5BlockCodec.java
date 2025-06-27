package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.io.IOException;

@NameConfig.Name(value = N5BlockCodec.TYPE)
public class N5BlockCodec implements Codec.ArrayCodec {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String TYPE = "n5bytes";

	private transient BytesCodec bytesCodec;

	private transient DatasetAttributes attributes;

	//TODO Caleb: Extract to factory that returns N5BlockCodec wrapper for datablockCodec
	@Override public void initialize(final DatasetAttributes attributes, final Codec.BytesCodec[] byteCodecs) {
		this.attributes = attributes;
		this.bytesCodec = new ConcatenatedBytesCodec(byteCodecs);
	}

	private <T> DataBlockCodec<T> getDataBlockCodec() {
		/*TODO: Consider an attributes.createDataBlockCodec() without parameters? */
		return N5Codecs.createDataBlockCodec(attributes.getDataType(), bytesCodec);
	}

	@Override
	public <T> DataBlock<T> decode(ReadData readData, long[] gridPosition) {

		return this.<T>getDataBlockCodec().decode(readData, gridPosition);
	}

	@Override
	public <T> ReadData encode(DataBlock<T> dataBlock) {

		return this.<T>getDataBlockCodec().encode(dataBlock);
	}

	@Override public long encodedSize(long size) {

		final int[] blockSize = attributes.getBlockSize();
		int headerSize = new N5Codecs.BlockHeader(blockSize, DataBlock.getNumElements(blockSize)).getSize();
		return headerSize + size;
	}

	@Override
	public String getType() {

		return TYPE;
	}
}
