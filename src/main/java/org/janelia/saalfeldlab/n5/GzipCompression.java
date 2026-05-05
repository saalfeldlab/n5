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

	/**
	 * Explicit equivalent of {@link java.util.zip.Deflater#DEFAULT_COMPRESSION}: zlib defines
	 * level 6 as "a default compromise between speed and compression." An explicit value is used
	 * instead of {@code DEFAULT_COMPRESSION} (-1) because -1 is not a valid level for Zarr codecs.
	 *
	 * @see <a href="https://www.zlib.net/manual.html">zlib Manual</a>
	 */
	private static final int N5_DEFAULT_GZIP_LEVEL = 6;

	@CompressionParameter
	@NameConfig.Parameter
	private final int level;

	/**
	 * This is not a NameConfig.Parameter because this parameter must not be
	 * serialized for zarr
	 */
	@CompressionParameter
	private final boolean useZlib;

	private final transient GzipParameters parameters = new GzipParameters();

	public GzipCompression() {

		this(N5_DEFAULT_GZIP_LEVEL);
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
