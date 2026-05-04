package org.janelia.saalfeldlab.n5;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@CompressionType("lz4")
@NameConfig.Name("lz4")
public class Lz4Compression implements Compression {

	private static final long serialVersionUID = -9071316415067427256L;

	@CompressionParameter
	@NameConfig.Parameter
	private final int blockSize;

	public Lz4Compression(final int blockSize) {

		this.blockSize = blockSize;
	}

	public Lz4Compression() {

		this(1 << 16);
	}

	@Override
	public boolean equals(final Object other) {

		if (other == null || other.getClass() != Lz4Compression.class)
			return false;
		else
			return blockSize == ((Lz4Compression)other).blockSize;
	}

	@Override
	public ReadData decode(final ReadData readData) {

		return ReadData.from(new LZ4BlockInputStream(readData.inputStream()));
	}

	@Override
	public ReadData encode(final ReadData readData) {
		return readData.encode(out -> new LZ4BlockOutputStream(out, blockSize));
	}
}
