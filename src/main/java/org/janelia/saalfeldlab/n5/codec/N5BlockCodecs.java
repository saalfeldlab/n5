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
package org.janelia.saalfeldlab.n5.codec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataBlock.DataBlockFactory;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.StringDataBlock;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import static org.janelia.saalfeldlab.n5.N5Exception.*;
import static org.janelia.saalfeldlab.n5.codec.N5BlockCodecs.BlockHeader.MODE_DEFAULT;
import static org.janelia.saalfeldlab.n5.codec.N5BlockCodecs.BlockHeader.MODE_OBJECT;
import static org.janelia.saalfeldlab.n5.codec.N5BlockCodecs.BlockHeader.MODE_VARLENGTH;
import static org.janelia.saalfeldlab.n5.codec.N5BlockCodecs.BlockHeader.headerSizeInBytes;

public class N5BlockCodecs {

	private static final BlockCodecFactory<byte[]>   BYTE   = c -> new DefaultBlockCodec<>(FlatArrayCodec.BYTE, ByteArrayDataBlock::new, c);
	private static final BlockCodecFactory<short[]>  SHORT  = c -> new DefaultBlockCodec<>(FlatArrayCodec.SHORT_BIG_ENDIAN, ShortArrayDataBlock::new, c);
	private static final BlockCodecFactory<int[]>    INT    = c -> new DefaultBlockCodec<>(FlatArrayCodec.INT_BIG_ENDIAN, IntArrayDataBlock::new, c);
	private static final BlockCodecFactory<long[]>   LONG   = c -> new DefaultBlockCodec<>(FlatArrayCodec.LONG_BIG_ENDIAN, LongArrayDataBlock::new, c);
	private static final BlockCodecFactory<float[]>  FLOAT  = c -> new DefaultBlockCodec<>(FlatArrayCodec.FLOAT_BIG_ENDIAN, FloatArrayDataBlock::new, c);
	private static final BlockCodecFactory<double[]> DOUBLE = c -> new DefaultBlockCodec<>(FlatArrayCodec.DOUBLE_BIG_ENDIAN, DoubleArrayDataBlock::new, c);
	private static final BlockCodecFactory<String[]> STRING = c -> new StringBlockCodec(c);
	private static final BlockCodecFactory<byte[]>   OBJECT = c -> new ObjectBlockCodec(c);

	private N5BlockCodecs() {}

	public static <T> BlockCodec<T> create(
			final DataType dataType,
			final DataCodec codec) {

		final BlockCodecFactory<?> factory;
		switch (dataType) {
		case UINT8:
		case INT8:
			factory = N5BlockCodecs.BYTE;
			break;
		case UINT16:
		case INT16:
			factory = N5BlockCodecs.SHORT;
			break;
		case UINT32:
		case INT32:
			factory = N5BlockCodecs.INT;
			break;
		case UINT64:
		case INT64:
			factory = N5BlockCodecs.LONG;
			break;
		case FLOAT32:
			factory = N5BlockCodecs.FLOAT;
			break;
		case FLOAT64:
			factory = N5BlockCodecs.DOUBLE;
			break;
		case STRING:
			factory = N5BlockCodecs.STRING;
			break;
		case OBJECT:
			factory = N5BlockCodecs.OBJECT;
			break;
		default:
			throw new IllegalArgumentException("Unsupported data type: " + dataType);
		}
		@SuppressWarnings("unchecked")
		final BlockCodecFactory<T> tFactory = (BlockCodecFactory<T>)factory;
		return tFactory.create(codec);
	}

	private interface BlockCodecFactory<T> {

		/**
		 * Create a {@link BlockCodec} that uses the specified {@code DataCodec}
		 * and de/serializes {@code DataBlock<T>} to N5 format.
		 *
		 * @return N5 {@code BlockCodec} using the specified {@code DataCodec}
		 */
		BlockCodec<T> create(DataCodec dataCodec);
	}

	abstract static class N5AbstractBlockCodec<T> implements BlockCodec<T> {

		final FlatArrayCodec<T> dataCodec;
		private final DataBlockFactory<T> dataBlockFactory;
		final DataCodec codec;

		N5AbstractBlockCodec(FlatArrayCodec<T> dataCodec, DataBlockFactory<T> dataBlockFactory, DataCodec codec) {
			this.dataCodec = dataCodec;
			this.dataBlockFactory = dataBlockFactory;
			this.codec = codec;
		}

		abstract BlockHeader createBlockHeader(final DataBlock<T> dataBlock, ReadData blockData) throws N5IOException;

		@Override
		public ReadData encode(DataBlock<T> dataBlock) throws N5IOException {
			return ReadData.from(out -> {
				final ReadData dataReadData = dataCodec.encode(dataBlock.getData());
				final BlockHeader header = createBlockHeader(dataBlock, dataReadData);

				header.writeTo(out);
				final ReadData encodedData = codec.encode(dataReadData);
				encodedData.writeTo(out);
			});
		}

		abstract BlockHeader decodeBlockHeader(final ReadData readData) throws N5IOException;

		@Override
		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition) throws N5IOException {

			final ReadData materializedData = readData.materialize();
			final BlockHeader header = decodeBlockHeader(materializedData);

			final int numElements = header.numElements();
			final long bodyLength = materializedData.length() - header.getSize();
			final ReadData bodyReadData = bodyLength > 0 ? materializedData.slice(header.getSize(), bodyLength) : ReadData.empty();
			final ReadData decodeData = codec.decode(bodyReadData);

			// the dataCodec knows the number of bytes per element
			final T data = dataCodec.decode(decodeData, numElements);
			return dataBlockFactory.createDataBlock(header.blockSize(), gridPosition, data);
		}
	}


	/**
	 * DataBlockCodec for all N5 data types, except STRING and OBJECT
	 */
	private static class DefaultBlockCodec<T> extends N5AbstractBlockCodec<T> {

		DefaultBlockCodec(
				final FlatArrayCodec<T> dataCodec,
				final DataBlockFactory<T> dataBlockFactory,
				final DataCodec codec) {

			super(dataCodec, dataBlockFactory, codec);
		}

		@Override
		protected BlockHeader createBlockHeader(final DataBlock<T> dataBlock, ReadData blockData) throws N5IOException {

			return new BlockHeader(dataBlock.getSize(), dataBlock.getNumElements());
		}

		@Override
		protected BlockHeader decodeBlockHeader(final ReadData readData) throws N5IOException {

			return BlockHeader.readFrom(readData, MODE_DEFAULT, MODE_VARLENGTH);
		}

		@Override
		public long encodedSize(final int[] blockSize) throws UnsupportedOperationException {
			if (codec instanceof DeterministicSizeDataCodec) {
				final int bytesPerElement = dataCodec.bytesPerElement();
				final int numElements = DataBlock.getNumElements(blockSize);
				final int headerSize = headerSizeInBytes(MODE_DEFAULT, blockSize.length);
				return headerSize + ((DeterministicSizeDataCodec) codec).encodedSize((long) numElements * bytesPerElement);
			} else {
				throw new UnsupportedOperationException();
			}
		}
	}

	/**
	 * DataBlockCodec for N5 data type STRING
	 */
	private static class StringBlockCodec extends N5AbstractBlockCodec<String[]> {

		StringBlockCodec(final DataCodec codec) {

			super(FlatArrayCodec.STRING, StringDataBlock::new, codec);
		}

		@Override
		protected BlockHeader createBlockHeader(final DataBlock<String[]> dataBlock, ReadData blockData) throws N5IOException {

			return new BlockHeader(MODE_VARLENGTH, dataBlock.getSize(), (int)blockData.length());
		}

		@Override
		protected BlockHeader decodeBlockHeader(final ReadData readData) throws N5IOException {

			return BlockHeader.readFrom(readData, MODE_DEFAULT, MODE_VARLENGTH);
		}
	}

	/**
	 * DataBlockCodec for N5 data type OBJECT
	 */
	private static class ObjectBlockCodec extends N5AbstractBlockCodec<byte[]> {

		ObjectBlockCodec(final DataCodec codec) {

			super(FlatArrayCodec.OBJECT, ByteArrayDataBlock::new, codec);
		}

		@Override
		protected BlockHeader createBlockHeader(DataBlock<byte[]> dataBlock, ReadData blockData) {

			return new BlockHeader(null, dataBlock.getNumElements());
		}

		@Override
		protected BlockHeader decodeBlockHeader(final ReadData readData) throws N5IOException {

			return BlockHeader.readFrom(readData, MODE_OBJECT);
		}
	}

	static class BlockHeader {

		public static final short MODE_DEFAULT = 0;
		public static final short MODE_VARLENGTH = 1;
		public static final short MODE_OBJECT = 2;

		private final short mode;
		private final int[] blockSize;
		private final int numElements;

		BlockHeader(final short mode, final int[] blockSize, final int numElements) {

			this.mode = mode;
			this.blockSize = blockSize;
			this.numElements = numElements;
		}

		BlockHeader(final int[] blockSize, final int numElements) {

			if (blockSize == null) {
				this.mode = MODE_OBJECT;
			} else if (DataBlock.getNumElements(blockSize) == numElements) {
				this.mode = MODE_DEFAULT;
			} else {
				this.mode = MODE_VARLENGTH;
			}
			this.blockSize = blockSize;
			this.numElements = numElements;
		}

		public int getSize() {

			return headerSizeInBytes(mode, blockSize == null ? 0 : blockSize.length);
		}

		public int[] blockSize() {

			return blockSize;
		}

		public int numElements() {

			return numElements;
		}

		private static int[] readBlockSize(final DataInputStream dis) throws N5IOException {

			try {
				final int nDim = dis.readShort();
				final int[] blockSize = new int[nDim];
				for (int d = 0; d < nDim; ++d)
					blockSize[d] = dis.readInt();
				return blockSize;
			} catch (IOException e) {
				throw new N5IOException(e);
			}
		}

		private static void writeBlockSize(final int[] blockSize, final DataOutputStream dos) throws N5IOException {

			try {
				dos.writeShort(blockSize.length);
				for (final int size : blockSize)
					dos.writeInt(size);
			} catch (IOException e) {
				throw new N5IOException(e);
			}
		}

		static int headerSizeInBytes(final short mode, final int numDimensions) {
			switch (mode) {
			case MODE_DEFAULT:
				return 2 + // 1 short for mode
						2 + // 1 short for blockSize.length
						4 * numDimensions; // 1 int for each blockSize element
			case MODE_VARLENGTH:
				return 2 +// 1 short for mode
						2 + // 1 short for blockSize.length
						4 * numDimensions + // 1 int for each blockSize dimension
						4; // 1 int for numElements
			case MODE_OBJECT:
				return 2 + // 1 short for mode
						4; // 1 int for numElements
			default:
				throw new N5Exception("unexpected mode: " + mode);
			}
		}

		void writeTo(final OutputStream out) throws N5IOException {

			try {
				final DataOutputStream dos = new DataOutputStream(out);
				dos.writeShort(mode);
				switch (mode) {
				case MODE_DEFAULT:// default
					writeBlockSize(blockSize, dos);
					break;
				case MODE_VARLENGTH:// varlength
					writeBlockSize(blockSize, dos);
					dos.writeInt(numElements);
					break;
				case MODE_OBJECT: // object
					dos.writeInt(numElements);
					break;
				default:
					throw new N5Exception("unexpected mode: " + mode);
				}
				dos.flush();
			} catch (IOException e) {
				throw new N5IOException(e);
			}

		}

		static BlockHeader readFrom(final ReadData readData, short... allowedModes) throws N5IOException, N5Exception {

			try {
				final DataInputStream dis = new DataInputStream(readData.inputStream());
				final short mode = dis.readShort();
				final int[] blockSize;
				final int numElements;
				switch (mode) {
				case MODE_DEFAULT:// default
					blockSize = readBlockSize(dis);
					numElements = DataBlock.getNumElements(blockSize);
					break;
				case MODE_VARLENGTH:// varlength
					blockSize = readBlockSize(dis);
					numElements = dis.readInt();
					break;
				case MODE_OBJECT: // object
					blockSize = null;
					numElements = dis.readInt();
					break;
				default:
					throw new N5Exception("Unexpected mode: " + mode);
				}

				boolean modeIsOk = allowedModes == null || allowedModes.length == 0;
				for (int i = 0; !modeIsOk && i < allowedModes.length; ++i) {
					if (mode == allowedModes[i]) {
						modeIsOk = true;
						break;
					}
				}
				if (!modeIsOk) {
					throw new N5Exception("Unexpected mode: " + mode);
				}

				return new BlockHeader(mode, blockSize, numElements);
			} catch (IOException e) {
				throw new N5IOException(e);
			}
		}
	}
}
