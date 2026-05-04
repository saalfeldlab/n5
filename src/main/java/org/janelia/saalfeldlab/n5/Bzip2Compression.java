package org.janelia.saalfeldlab.n5;

import java.io.IOException;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@CompressionType("bzip2")
@NameConfig.Name("bzip2")
public class Bzip2Compression implements Compression {

	private static final long serialVersionUID = -4873117458390529118L;

	@CompressionParameter
	@NameConfig.Parameter
	private final int blockSize;

	public Bzip2Compression(final int blockSize) {

		this.blockSize = blockSize;
	}

	public Bzip2Compression() {

		this(BZip2CompressorOutputStream.MAX_BLOCKSIZE);
	}

	@Override
	public boolean equals(final Object other) {

		if (other == null || other.getClass() != Bzip2Compression.class)
			return false;
		else
			return blockSize == ((Bzip2Compression)other).blockSize;
	}

	@Override
	public ReadData decode(final ReadData readData) throws N5IOException {
		try {
			return ReadData.from(new BZip2CompressorInputStream(readData.inputStream()));
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

	@Override
	public ReadData encode(final ReadData readData) {
		return readData.encode(out -> new BZip2CompressorOutputStream(out, blockSize));
	}
}
