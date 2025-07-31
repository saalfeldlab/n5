package org.janelia.saalfeldlab.n5.codec;

public class ConcatenatedDeterministicSizeBytesCodec extends ConcatenatedBytesCodec implements DeterministicSizeBytesCodec {

	private final DeterministicSizeBytesCodec[] codecs;

	ConcatenatedDeterministicSizeBytesCodec(final DeterministicSizeBytesCodec[] codecs) {

		super(codecs);
		this.codecs = codecs;
	}

	@Override
	public long encodedSize(long size) {

		for (DeterministicSizeBytesCodec codec : codecs) {
			size = codec.encodedSize(size);
		}
		return size;
	}
}
