package org.janelia.saalfeldlab.n5.codec;

public class IndexCodecAdapter {

	private final BlockCodecInfo arrayCodec;
	private final DeterministicSizeCodec[] codecs;

	public IndexCodecAdapter(final BlockCodecInfo arrayCodec, final DeterministicSizeCodec... codecs) {

		this.arrayCodec = arrayCodec;
		this.codecs = codecs;
	}

	public BlockCodecInfo getArrayCodec() {

		return arrayCodec;
	}

	public DataCodec[] getCodecs() {

		final DataCodec[] bytesCodecs = new DataCodec[codecs.length];
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

	public static IndexCodecAdapter create(final CodecInfo... codecs) {
		if (codecs == null || codecs.length == 0)
			return new IndexCodecAdapter(new RawBlockCodecInfo());

		if (codecs[0] instanceof BlockCodecInfo)
			return new IndexCodecAdapter((BlockCodecInfo)codecs[0]);

		final DeterministicSizeCodec[] indexCodecs = new DeterministicSizeCodec[codecs.length];
		System.arraycopy(codecs, 0, indexCodecs, 0, codecs.length);
		return new IndexCodecAdapter(new RawBlockCodecInfo(), indexCodecs);
	}
}
