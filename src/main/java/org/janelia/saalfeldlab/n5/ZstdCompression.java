package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;

/**
 * Zstandard compression for N5
 * 
 * Implementation wrapper around Apache Commons Compress
 * 
 * Note that zstd-jni is an optional dependency of Apache Commons Compress.
 * However, it is required for this class, ZstdCompression, to work
 * https://github.com/luben/zstd-jni
 * 
 * Add the following dependency entry under to the Maven pom.xml
 * 
 * <pre>{@code
 *		<dependency>
 *		    <groupId>com.github.luben</groupId>
 *		    <artifactId>zstd-jni</artifactId>
 *		    <version>1.5.5-10</version>
 *		</dependency>
 * }</pre>
 * 
 * See the Zstandard manual for details on parameters.
 * https://facebook.github.io/zstd/zstd_manual.html
 * 
 * @author mkitti
 *
 */
@CompressionType("zstd")
public class ZstdCompression implements DefaultBlockReader, DefaultBlockWriter, Compression {
	
	private static final long serialVersionUID = 8592416400988371189L;

	/**
	 * Compression level
	 * 
	 * Standard compression level is between 1 and 22
	 * Negative compression levels offer speed
	 * 
	 * Note that zarr-developers/numcodecs defaults to 1
	 * 
	 * Default: 3 (see ZSTD_CLEVEL_DEFAULT)
	 */
	@CompressionParameter
	private int level = 3;
	
	/**
	 * Default compression level from zstd.h
	 */
	public static final int ZSTD_CLEVEL_DEFAULT = 3;
	
	/**
	 * Create Zstandard compression with level equal to the constant ZSTD_CLEVEL_DEFAULT (value: {@value ZstdCompression#ZSTD_CLEVEL_DEFAULT})
	 */
	public ZstdCompression() {
		this.level = ZSTD_CLEVEL_DEFAULT;
	}
	
	/**
	 * @param level The standard compression levels are normally between 1 to 22. Negative compression levels offer greater speed.
	 */
	public ZstdCompression(int level) {
		this.level = level;
	}

	@Override
	public BlockReader getReader() {
		return this;
	}

	@Override
	public BlockWriter getWriter() {
		return this;
	}

	@Override
	public OutputStream getOutputStream(OutputStream out) throws IOException {
		ZstdCompressorOutputStream zstdOut = new ZstdCompressorOutputStream(out, level);
		return zstdOut;
	}

	@Override
	public InputStream getInputStream(InputStream in) throws IOException {
		ZstdCompressorInputStream zstdIn = new ZstdCompressorInputStream(in);
		return zstdIn;
	}

}
