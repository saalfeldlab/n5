package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.readdata.ReadData;

import java.io.IOException;

class ConcatenatedBytesCodec implements Codec.BytesCodec {

	private final BytesCodec[] codecs;

	ConcatenatedBytesCodec(final BytesCodec... codecs) {
		this.codecs = codecs;
	}

	@Override public ReadData encode(ReadData readData) throws IOException {

		ReadData encodeData = readData;
		if (codecs != null) {
			for (Codec.BytesCodec codec : codecs) {
				encodeData = codec.encode(encodeData);
			}
		}
		return encodeData;
	}

	@Override public ReadData decode(ReadData readData) throws IOException {
		ReadData decodeData = readData;
		if (codecs != null) {
			for (int i = codecs.length - 1; i >= 0; i--) {
				final BytesCodec codec = codecs[i];
				decodeData = codec.decode(decodeData);
			}
		}
		return decodeData;
	}

	@Override public String getType() {

		return "internal-concatenated-codecs";
	}
}
