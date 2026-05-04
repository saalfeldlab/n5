package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@CompressionType("gzip")
@NameConfig.Name("gzip")
public class GzipCompression implements Compression {

	private static final long serialVersionUID = 8630847239813334263L;

	@CompressionParameter
	@NameConfig.Parameter
	//TODO Caleb: How to handle serialization of parameter-less constructor.
	// For N5 the default is -1.
	// For zarr the range is 0-9 and is required.
	// How to map -1 to some default (1?) when serializing to zarr?
	private final int level;

	@CompressionParameter
	@NameConfig.Parameter(optional = true)
	private final boolean useZlib;

	private final transient GzipParameters parameters = new GzipParameters();

	public GzipCompression() {

		this(Deflater.DEFAULT_COMPRESSION);
	}

	public GzipCompression(final int level) {

		this(level, false);
	}

	public GzipCompression(final int level, final boolean useZlib) {

		this.level = level;
		this.useZlib = useZlib;
	}

	@Override
	public boolean equals(final Object other) {

		if (other == null || other.getClass() != GzipCompression.class)
			return false;
		else {
			final GzipCompression gz = ((GzipCompression)other);
			return useZlib == gz.useZlib && level == gz.level;
		}
	}

	private InputStream decode(final InputStream in) throws IOException {
		if (useZlib) {
			return new InflaterInputStream(in);
		} else {
			return GzipCompressorInputStream.builder()
					.setInputStream(in)
					.setDecompressConcatenated(true)
					.get();
		}
	}

	@Override
	public ReadData decode(final ReadData readData) throws N5IOException {

		try {
			return ReadData.from(decode(readData.inputStream()));
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

	@Override
	public ReadData encode(final ReadData readData)  {
		if (useZlib) {
			return readData.encode(out -> new DeflaterOutputStream(out, new Deflater(level)));
		} else {
			return readData.encode(out -> {
				parameters.setCompressionLevel(level);
				return new GzipCompressorOutputStream(out, parameters);
			});
		}
	}
}
