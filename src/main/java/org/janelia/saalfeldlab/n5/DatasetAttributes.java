/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;

import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.ArrayCodec;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.N5ArrayCodec;

/**
 * Mandatory dataset attributes:
 *
 * <ol>
 * <li>long[] : dimensions</li>
 * <li>int[] : blockSize</li>
 * <li>{@link DataType} : dataType</li>
 * <li>{@link Compression} : compression</li>
 * </ol>
 *
 * @author Stephan Saalfeld
 *
 */
public class DatasetAttributes implements Serializable {

	private static final long serialVersionUID = -4521467080388947553L;

	public static final String DIMENSIONS_KEY = "dimensions";
	public static final String BLOCK_SIZE_KEY = "blockSize";
	public static final String DATA_TYPE_KEY = "dataType";
	public static final String COMPRESSION_KEY = "compression";

	/* version 0 */
	protected static final String compressionTypeKey = "compressionType";

	private final long[] dimensions;
	private final int[] blockSize;
	private final DataType dataType;

	private final ArrayCodec arrayCodec;
	private final BytesCodec[] byteCodecs;

	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final ArrayCodec arrayCodec,
			final BytesCodec... codecs) {

		this.dimensions = dimensions;
		this.blockSize = blockSize;
		this.dataType = dataType;

		this.arrayCodec = arrayCodec == null ? defaultArrayCodec() : arrayCodec;
		byteCodecs = Arrays.stream(codecs).filter(it -> !(it instanceof RawCompression)).toArray(BytesCodec[]::new);

//		if (filteredCodecs.length == 0) {
//			byteCodecs = new BytesCodec[]{};
//			arrayCodec = defaultArrayCodec();
//		} else if (filteredCodecs.length == 1 && filteredCodecs[0] instanceof Compression) {
//			final BytesCodec compression = (BytesCodec)filteredCodecs[0];
//			byteCodecs = compression instanceof RawCompression ? new BytesCodec[]{} : new BytesCodec[]{compression};
//			arrayCodec = defaultArrayCodec();
//		} else {
//			if (!(filteredCodecs[0] instanceof ArrayCodec))
//				throw new N5Exception("Expected first element of filteredCodecs to be ArrayCodec, but was: " + filteredCodecs[0].getClass());
//
//			if (Arrays.stream(filteredCodecs).filter(c -> c instanceof ArrayCodec).count() > 1)
//				throw new N5Exception("Multiple ArrayCodecs found. Only one is allowed.");
//
//			arrayCodec = (ArrayCodec)filteredCodecs[0];
//			byteCodecs = Stream.of(filteredCodecs)
//					.skip(1)
//					.filter(c -> c instanceof BytesCodec)
//					.toArray(BytesCodec[]::new);
//		}

		this.arrayCodec.initialize(this, byteCodecs);
	}

	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final BytesCodec compression) {

		this(dimensions, blockSize, dataType, null, compression);
	}

	protected ArrayCodec defaultArrayCodec() {
		return new N5ArrayCodec();
	}

	public long[] getDimensions() {

		return dimensions;
	}

	public int getNumDimensions() {

		return dimensions.length;
	}

	public int[] getBlockSize() {

		return blockSize;
	}

	public Compression getCompression() {

		return Arrays.stream(byteCodecs)
				.filter(it -> it instanceof Compression)
				.map(it -> (Compression)it)
				.findFirst()
				.orElse(new RawCompression());
	}

	public DataType getDataType() {

		return dataType;
	}

	/**
	 * Get the {@link ArrayCodec} for this dataset.
	 *
	 * @return the {@code ArrayCodec} for this dataset
	 */
	public ArrayCodec getArrayCodec() {

		return arrayCodec;
	}

	public HashMap<String, Object> asMap() {

		final HashMap<String, Object> map = new HashMap<>();
		map.put(DIMENSIONS_KEY, dimensions);
		map.put(BLOCK_SIZE_KEY, blockSize);
		map.put(DATA_TYPE_KEY, dataType);
		map.put(COMPRESSION_KEY, getCompression());
		return map;
	}

	static DatasetAttributes from(
			final long[] dimensions,
			final DataType dataType,
			int[] blockSize,
			Compression compression,
			final String compressionVersion0Name) {

		if (blockSize == null)
			blockSize = Arrays.stream(dimensions).mapToInt(a -> (int)a).toArray();

		/* version 0 */
		if (compression == null) {
			switch (compressionVersion0Name) {
			case "raw":
				compression = new RawCompression();
				break;
			case "gzip":
				compression = new GzipCompression();
				break;
			case "bzip2":
				compression = new Bzip2Compression();
				break;
			case "lz4":
				compression = new Lz4Compression();
				break;
			case "xz":
				compression = new XzCompression();
				break;
			}
		}

		return new DatasetAttributes(dimensions, blockSize, dataType, compression);
	}
}
