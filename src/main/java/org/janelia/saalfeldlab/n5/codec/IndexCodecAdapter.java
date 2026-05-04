package org.janelia.saalfeldlab.n5.codec;

public class IndexCodecAdapter {

	private final BlockCodecInfo blockCodecInfo;
	private final DeterministicSizeCodecInfo[] dataCodecs;

	public IndexCodecAdapter(final BlockCodecInfo blockCodecInfo, final DeterministicSizeCodecInfo... dataCodecs) {

		this.blockCodecInfo = blockCodecInfo;
		this.dataCodecs = dataCodecs;
	}

	public BlockCodecInfo getBlockCodecInfo() {

		return blockCodecInfo;
	}

	public DataCodecInfo[] getDataCodecs() {

		final DataCodecInfo[] dataCodecs = new DataCodecInfo[this.dataCodecs.length];
		System.arraycopy(this.dataCodecs, 0, dataCodecs, 0, this.dataCodecs.length);
		return dataCodecs;
	}

	public long encodedSize(long initialSize) {
		long totalNumBytes = initialSize;
		for (DeterministicSizeCodecInfo codec : dataCodecs) {
			totalNumBytes = codec.encodedSize(totalNumBytes);
		}
		return totalNumBytes;
	}

	public static IndexCodecAdapter create(final CodecInfo... codecs) {
		if (codecs == null || codecs.length == 0)
			return new IndexCodecAdapter(new RawBlockCodecInfo());

		if (codecs[0] instanceof BlockCodecInfo)
			return new IndexCodecAdapter((BlockCodecInfo)codecs[0]);

		final DeterministicSizeCodecInfo[] indexCodecs = new DeterministicSizeCodecInfo[codecs.length];
		System.arraycopy(codecs, 0, indexCodecs, 0, codecs.length);
		return new IndexCodecAdapter(new RawBlockCodecInfo(), indexCodecs);
	}
}
