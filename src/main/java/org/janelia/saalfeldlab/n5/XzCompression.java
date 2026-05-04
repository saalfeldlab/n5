package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@CompressionType("xz")
@NameConfig.Name("xz")
public class XzCompression implements Compression {

	private static final long serialVersionUID = -7272153943564743774L;

	@CompressionParameter
	@NameConfig.Parameter
	private final int preset;

	public XzCompression(final int preset) {

		this.preset = preset;
	}

	public XzCompression() {

		this(6);
	}

	@Override
	public boolean equals(final Object other) {

		if (other == null || other.getClass() != XzCompression.class)
			return false;
		else
			return preset == ((XzCompression)other).preset;
	}

	@Override
	public ReadData decode(final ReadData readData) throws N5IOException {

		try {
			return ReadData.from(new XZCompressorInputStream(readData.inputStream()));
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

	@Override
	public ReadData encode(final ReadData readData) {
		return readData.encode(out -> new XZCompressorOutputStream(out, preset));
	}
}
