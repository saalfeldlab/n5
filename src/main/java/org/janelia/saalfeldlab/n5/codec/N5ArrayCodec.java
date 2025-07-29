package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name(value = N5ArrayCodec.TYPE)
public class N5ArrayCodec implements ArrayCodec {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String TYPE = "n5bytes";

	@Override
	public String getType() {

		return TYPE;
	}

	@Override
	public <T> DataBlockSerializer<T> initialize(final DatasetAttributes attributes, final BytesCodec... bytesCodecs) {
		return N5DataBlockSerializers.create(attributes.getDataType(), BytesCodec.concatenate(bytesCodecs));
	}

	@Override
	public long encodedSize(long size) {
		throw new UnsupportedOperationException("TODO");

		// TODO: As the header size depends on the DataType, it would make most
		//       sense to move this into DataBlockSerializer<T> or a sub-class.
		//       I don't understand enough about how it is supposed to be used
		//       to make a decision...

//		final int[] blockSize = attributes.getBlockSize();
//		final int headerSize = new N5DataBlockSerializers.BlockHeader(blockSize, DataBlock.getNumElements(blockSize)).getSize();
//		return headerSize + size;
	}

	// TODO: If we override encodedSize() we should probably also override decodedSize() ???
	@Override
	public long decodedSize(long size) {
		throw new UnsupportedOperationException("TODO");
	}
}
