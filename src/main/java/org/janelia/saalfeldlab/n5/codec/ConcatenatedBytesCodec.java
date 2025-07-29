package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.readdata.ReadData;

class ConcatenatedBytesCodec implements BytesCodec {

	private final BytesCodec[] codecs;

	ConcatenatedBytesCodec(final BytesCodec[] codecs) {

		if (codecs == null) {
			throw new NullPointerException();
		}
		this.codecs = codecs;
	}

	@Override
	public ReadData encode(ReadData readData) {

		for (BytesCodec codec : codecs) {
			readData = codec.encode(readData);
		}
		return readData;
	}

	@Override
	public ReadData decode(ReadData readData) {

		for (int i = codecs.length - 1; i >= 0; i--) {
			final BytesCodec codec = codecs[i];
			readData = codec.decode(readData);
		}
		return readData;
	}

	@Override
	public String getType() {

		return "internal-concatenated-codecs";
	}
}
