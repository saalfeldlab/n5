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
import java.io.InputStream;
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
import static org.janelia.saalfeldlab.n5.codec.N5DataBlockSerializers.BlockHeader.MODE_DEFAULT;
import static org.janelia.saalfeldlab.n5.codec.N5DataBlockSerializers.BlockHeader.MODE_OBJECT;
import static org.janelia.saalfeldlab.n5.codec.N5DataBlockSerializers.BlockHeader.MODE_VARLENGTH;

public class N5DataBlockSerializers {

	private static final DataBlockSerializerFactory<byte[]>   BYTE   = c -> new DefaultBlockCodec<>(FlatArraySerializer.BYTE, ByteArrayDataBlock::new, c);
	private static final DataBlockSerializerFactory<short[]>  SHORT  = c -> new DefaultBlockCodec<>(FlatArraySerializer.SHORT_BIG_ENDIAN, ShortArrayDataBlock::new, c);
	private static final DataBlockSerializerFactory<int[]>    INT    = c -> new DefaultBlockCodec<>(FlatArraySerializer.INT_BIG_ENDIAN, IntArrayDataBlock::new, c);
	private static final DataBlockSerializerFactory<long[]>   LONG   = c -> new DefaultBlockCodec<>(FlatArraySerializer.LONG_BIG_ENDIAN, LongArrayDataBlock::new, c);
	private static final DataBlockSerializerFactory<float[]>  FLOAT  = c -> new DefaultBlockCodec<>(FlatArraySerializer.FLOAT_BIG_ENDIAN, FloatArrayDataBlock::new, c);
	private static final DataBlockSerializerFactory<double[]> DOUBLE = c -> new DefaultBlockCodec<>(FlatArraySerializer.DOUBLE_BIG_ENDIAN, DoubleArrayDataBlock::new, c);
	private static final DataBlockSerializerFactory<String[]> STRING = c -> new StringBlockCodec(c);
	private static final DataBlockSerializerFactory<byte[]>   OBJECT = c -> new ObjectBlockCodec(c);

	private N5DataBlockSerializers() {}

	public static <T> BlockCodec<T> create(
			final DataType dataType,
			final BytesCodec codec) {

		final DataBlockSerializerFactory<?> factory;
		switch (dataType) {
		case UINT8:
		case INT8:
			factory = N5DataBlockSerializers.BYTE;
			break;
		case UINT16:
		case INT16:
			factory = N5DataBlockSerializers.SHORT;
			break;
		case UINT32:
		case INT32:
			factory = N5DataBlockSerializers.INT;
			break;
		case UINT64:
		case INT64:
			factory = N5DataBlockSerializers.LONG;
			break;
		case FLOAT32:
			factory = N5DataBlockSerializers.FLOAT;
			break;
		case FLOAT64:
			factory = N5DataBlockSerializers.DOUBLE;
			break;
		case STRING:
			factory = N5DataBlockSerializers.STRING;
			break;
		case OBJECT:
			factory = N5DataBlockSerializers.OBJECT;
			break;
		default:
			throw new IllegalArgumentException("Unsupported data type: " + dataType);
		}
		@SuppressWarnings("unchecked")
		final DataBlockSerializerFactory<T> tFactory = (DataBlockSerializerFactory<T>)factory;
		return tFactory.create(codec);
	}

	private interface DataBlockSerializerFactory<T> {

		/**
		 * Create a {@link BlockCodec} that uses the specified {@code
		 * BytesCodec} and de/serializes {@code DataBlock<T>} to N5 format.
		 *
		 * @return N5 {@code DataBlockSerializer} for the specified {@code codec}
		 */
		BlockCodec<T> create(BytesCodec codec);
	}

	abstract static class N5AbstractBlockCodec<T> implements BlockCodec<T> {

		private final FlatArraySerializer<T> dataCodec;
		private final DataBlockFactory<T> dataBlockFactory;
		private final BytesCodec codec;

		N5AbstractBlockCodec(FlatArraySerializer<T> dataCodec, DataBlockFactory<T> dataBlockFactory, BytesCodec codec) {
			this.dataCodec = dataCodec;
			this.dataBlockFactory = dataBlockFactory;
			this.codec = codec;
		}

		abstract BlockHeader createBlockHeader(final DataBlock<T> dataBlock, ReadData blockData) throws N5IOException;

		@Override
		public ReadData encode(DataBlock<T> dataBlock) throws N5IOException {
			return ReadData.from(out -> {
				final ReadData dataReadData = dataCodec.serialize(dataBlock.getData());
				final BlockHeader header = createBlockHeader(dataBlock, dataReadData);

				header.writeTo(out);
				final ReadData encodedData = codec.encode(dataReadData);
				encodedData.writeTo(out);
			});
		}

		abstract BlockHeader decodeBlockHeader(final InputStream in) throws N5IOException;

		@Override
		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition) throws N5IOException {

			try(final InputStream in = readData.inputStream()) {
				final BlockHeader header = decodeBlockHeader(in);

				final int numElements = header.numElements();
				final ReadData decodeData = codec.decode(ReadData.from(in));

				// the dataCodec knows the number of bytes per element
				final T data = dataCodec.deserialize(decodeData, numElements);
				return dataBlockFactory.createDataBlock(header.blockSize(), gridPosition, data);
			} catch (IOException e) {
				throw new N5IOException(e);
			}
		}
	}


	/**
	 * DataBlockCodec for all N5 data types, except STRING and OBJECT
	 */
	private static class DefaultBlockCodec<T> extends N5AbstractBlockCodec<T> {

		DefaultBlockCodec(
				final FlatArraySerializer<T> dataCodec,
				final DataBlockFactory<T> dataBlockFactory,
				final BytesCodec codec) {

			super(dataCodec, dataBlockFactory, codec);
		}

		@Override
		protected BlockHeader createBlockHeader(final DataBlock<T> dataBlock, ReadData blockData) throws N5IOException {

			return new BlockHeader(dataBlock.getSize(), dataBlock.getNumElements());
		}

		@Override
		protected BlockHeader decodeBlockHeader(final InputStream in) throws N5IOException {

			return BlockHeader.readFrom(in, MODE_DEFAULT, MODE_VARLENGTH);
		}
	}

	/**
	 * DataBlockCodec for N5 data type STRING
	 */
	private static class StringBlockCodec extends N5AbstractBlockCodec<String[]> {

		StringBlockCodec(final BytesCodec codec) {

			super(FlatArraySerializer.STRING, StringDataBlock::new, codec);
		}

		@Override
		protected BlockHeader createBlockHeader(final DataBlock<String[]> dataBlock, ReadData blockData) throws N5IOException {

			return new BlockHeader(MODE_VARLENGTH, dataBlock.getSize(), (int)blockData.length());
		}

		@Override
		protected BlockHeader decodeBlockHeader(final InputStream in) throws N5IOException {

			return BlockHeader.readFrom(in, MODE_DEFAULT, MODE_VARLENGTH);
		}
	}

	/**
	 * DataBlockCodec for N5 data type OBJECT
	 */
	private static class ObjectBlockCodec extends N5AbstractBlockCodec<byte[]> {

		ObjectBlockCodec(final BytesCodec codec) {

			super(FlatArraySerializer.OBJECT, ByteArrayDataBlock::new, codec);
		}

		@Override
		protected BlockHeader createBlockHeader(DataBlock<byte[]> dataBlock, ReadData blockData) {

			return new BlockHeader(null, dataBlock.getNumElements());
		}

		@Override
		protected BlockHeader decodeBlockHeader(final InputStream in) throws N5IOException {

			return BlockHeader.readFrom(in, MODE_OBJECT);
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

			switch (mode) {
				case MODE_DEFAULT:
					return 2 + 4 * blockSize.length;
				case MODE_VARLENGTH:
					return 2 + 4 * blockSize.length + 4;
				case MODE_OBJECT:
					return 2 + 4;
				default:
					throw new IllegalArgumentException("Unexpected mode: " + mode);
			}
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

		static BlockHeader readFrom(final InputStream in, short... allowedModes) throws N5IOException, N5Exception {

			try {
				final DataInputStream dis = new DataInputStream(in);
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
