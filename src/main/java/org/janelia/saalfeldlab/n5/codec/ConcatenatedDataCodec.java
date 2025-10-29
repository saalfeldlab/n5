package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.readdata.ReadData;

class ConcatenatedDataCodec implements DataCodec {

	private final DataCodec[] codecs;

	ConcatenatedDataCodec(final DataCodec[] codecs) {

		if (codecs == null) {
			throw new NullPointerException();
		}
		this.codecs = codecs;
	}

	@Override
	public ReadData encode(ReadData readData) {

		for (DataCodec codec : codecs) {
			readData = codec.encode(readData);
		}
		return readData;
	}

	@Override
	public ReadData decode(ReadData readData) {

		for (int i = codecs.length - 1; i >= 0; i--) {
			final DataCodec codec = codecs[i];
			readData = codec.decode(readData);
		}
		return readData;
	}
}
