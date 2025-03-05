package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import java.io.IOException;

public class N5BlockCodec<T> implements DataBlockCodec<T> {

	private DataBlockCodec<T> dataBlockCodec;

	public	N5BlockCodec(DataBlockCodec<T> dataBlockCodec) {
		this.dataBlockCodec = dataBlockCodec;
	}

	@Override public DataBlock<T> decode(ReadData readData, long[] gridPosition) throws IOException {

		return dataBlockCodec.decode(readData, gridPosition);
	}

	@Override public ReadData encode(DataBlock<T> dataBlock) throws IOException {

		return dataBlockCodec.encode(dataBlock);
	}

	public static class N5BlockCodecFactory {

		private N5BlockCodecFactory() {}

		public static N5BlockCodec<?> fromDatasetAttributes(final DatasetAttributes attributes) {

			final DataBlockCodec<?> dataBlockCodec = N5Codecs.createDataBlockCodec(attributes.getDataType(), attributes.getCompression());
			return new N5BlockCodec<>(dataBlockCodec);
		}
	}
}
