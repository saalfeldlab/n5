package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.io.IOException;

@NameConfig.Name(value = N5BlockCodec.TYPE)
public class N5BlockCodec<T> implements Codec.ArrayCodec<T> {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String TYPE = "n5bytes";

	private DataBlockCodec<T> dataBlockCodec;
	private DatasetAttributes attributes;


	//TODO Caleb: Extract to factory that returns N5BlockCodec wrapper for datablockCodec
	@Override public void setDatasetAttributes(final DatasetAttributes attributes, final Codec.BytesCodec... codecs) {
		/*TODO: Consider an attributes.createDataBlockCodec() without parameters? */
		this.attributes = attributes;
		final BytesCodec[] byteCodecs = codecs == null ? attributes.getCodecs() : codecs;
		this.dataBlockCodec = N5Codecs.createDataBlockCodec(attributes.getDataType(), byteCodecs);
	}

	@Override public DataBlock<T> decode(ReadData readData, long[] gridPosition) throws IOException {

		return dataBlockCodec.decode(readData, gridPosition);
	}

	@Override public ReadData encode(DataBlock<T> dataBlock) throws IOException {

		return dataBlockCodec.encode(dataBlock);
	}

	@Override
	public String getType() {

		return TYPE;
	}
}
