package org.janelia.saalfeldlab.n5.codec;

public class IndexCodecAdapter {

	private final ArrayCodec arrayCodec;
	private final DeterministicSizeCodec[] codecs;

	public IndexCodecAdapter(final ArrayCodec arrayCodec, final DeterministicSizeCodec... codecs) {

		this.arrayCodec = arrayCodec;
		this.codecs = codecs;
	}

	public ArrayCodec getArrayCodec() {

		return arrayCodec;
	}

	public BytesCodec[] getCodecs() {

		final BytesCodec[] bytesCodecs = new BytesCodec[codecs.length];
		System.arraycopy(codecs, 0, bytesCodecs, 0, codecs.length);
		return bytesCodecs;
	}

	public long encodedSize(long initialSize) {
		long totalNumBytes = initialSize;
		for (DeterministicSizeCodec codec : codecs) {
			totalNumBytes = codec.encodedSize(totalNumBytes);
		}
		return totalNumBytes;
	}

	public static IndexCodecAdapter create(final Codec... codecs) {
		if (codecs == null || codecs.length == 0)
			return new IndexCodecAdapter(new RawBytesArrayCodec());

		if (codecs[0] instanceof ArrayCodec)
			return new IndexCodecAdapter((ArrayCodec)codecs[0]);

		final DeterministicSizeCodec[] indexCodecs = new DeterministicSizeCodec[codecs.length];
		System.arraycopy(codecs, 0, indexCodecs, 0, codecs.length);
		return new IndexCodecAdapter(new RawBytesArrayCodec(), indexCodecs);
	}
}
