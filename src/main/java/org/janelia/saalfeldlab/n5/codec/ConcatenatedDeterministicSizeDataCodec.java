package org.janelia.saalfeldlab.n5.codec;

class ConcatenatedDeterministicSizeDataCodec extends ConcatenatedDataCodec implements DeterministicSizeDataCodec {

	private final DeterministicSizeDataCodec[] codecs;

	ConcatenatedDeterministicSizeDataCodec(final DeterministicSizeDataCodec[] codecs) {

		super(codecs);
		this.codecs = codecs;
	}

	@Override
	public long encodedSize(long size) {

		for (DeterministicSizeDataCodec codec : codecs) {
			size = codec.encodedSize(size);
		}
		return size;
	}
}
