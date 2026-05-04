package org.janelia.saalfeldlab.n5;

import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeDataCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@CompressionType("raw")
@NameConfig.Name("raw")
public class RawCompression implements Compression, DeterministicSizeDataCodec {

	private static final long serialVersionUID = 7526445806847086477L;

	@Override
	public boolean equals(final Object other) {

		return other != null && other.getClass() == RawCompression.class;
	}

	@Override
	public ReadData encode(final ReadData readData) {
		return readData;
	}

	@Override
	public ReadData decode(final ReadData readData) {
		return readData;
	}

	@Override
	public long encodedSize(final long size) {
		return size;
	}
}
