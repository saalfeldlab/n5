package org.janelia.saalfeldlab.n5;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import static org.janelia.saalfeldlab.n5.Codecs.ChunkHeader.MODE_DEFAULT;
import static org.janelia.saalfeldlab.n5.Codecs.ChunkHeader.MODE_OBJECT;
import static org.janelia.saalfeldlab.n5.Codecs.ChunkHeader.MODE_VARLENGTH;

public class Codecs {


	public interface DataBlockCodec<T> {

		ReadData encode(DataBlock<T> dataBlock, Compression compression) throws IOException;

		DataBlock<T> decode(ReadData readData, long[] gridPosition, Compression compression) throws IOException;

		DataBlockCodec<byte[]>   BYTE   = new DefaultDataBlockCodec<>(DataCodec.BYTE, ByteArrayDataBlock::new);
		DataBlockCodec<short[]>  SHORT  = new DefaultDataBlockCodec<>(DataCodec.SHORT, ShortArrayDataBlock::new);
		DataBlockCodec<int[]>    INT    = new DefaultDataBlockCodec<>(DataCodec.INT, IntArrayDataBlock::new);
		DataBlockCodec<long[]>   LONG   = new DefaultDataBlockCodec<>(DataCodec.LONG, LongArrayDataBlock::new);
		DataBlockCodec<float[]>  FLOAT  = new DefaultDataBlockCodec<>(DataCodec.FLOAT, FloatArrayDataBlock::new);
		DataBlockCodec<double[]> DOUBLE = new DefaultDataBlockCodec<>(DataCodec.DOUBLE, DoubleArrayDataBlock::new);
		DataBlockCodec<String[]> STRING = new StringDataBlockCodec();
		DataBlockCodec<byte[]>   OBJECT = new ObjectDataBlockCodec();
	}


	/**
	 * DataBlockCodec for all N5 data types, except STRING and OBJECT
	 */
	static class DefaultDataBlockCodec<T> implements DataBlockCodec<T> {

		interface DataBlockFactory<T> {

			DataBlock<T> createDataBlock(int[] blockSize, long[] gridPosition, T data);
		}

		private final DataCodec<T> dataCodec;

		private final DataBlockFactory<T> dataBlockFactory;

		DefaultDataBlockCodec(
				final DataCodec<T> dataCodec,
				final DataBlockFactory<T> dataBlockFactory) {
			this.dataCodec = dataCodec;
			this.dataBlockFactory = dataBlockFactory;
		}

		@Override
		public ReadData encode(final DataBlock<T> dataBlock, final Compression compression) throws IOException {
			return ReadData.from(out -> {
				new ChunkHeader(dataBlock.getSize(), dataBlock.getNumElements()).writeTo(out);
				compression.encode(dataCodec.serialize(dataBlock.getData())).writeTo(out);
				out.flush();
			});
		}

		@Override
		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition, final Compression compression) throws IOException {
			try(final InputStream in = readData.inputStream()) {
				final ChunkHeader header = ChunkHeader.readFrom(in, MODE_DEFAULT, MODE_VARLENGTH);
				final T data = dataCodec.createData(header.numElements());
				final int numBytes = header.numElements() * dataCodec.bytesPerElement();
				final ReadData decompressed = compression.decode(ReadData.from(in), numBytes);
				dataCodec.deserialize(decompressed, data);
				return dataBlockFactory.createDataBlock(header.blockSize(), gridPosition, data);
			}
		}
	}

	/**
	 * TODO javadoc
	 */
	static class StringDataBlockCodec implements DataBlockCodec<String[]> {

		private static final Charset ENCODING = StandardCharsets.UTF_8;
		private static final String NULLCHAR = "\0";

		@Override
		public ReadData encode(final DataBlock<String[]> dataBlock, final Compression compression) throws IOException {
			return ReadData.from(out -> {
				final String flattenedArray = String.join(NULLCHAR, dataBlock.getData()) + NULLCHAR;
				final byte[] serializedData = flattenedArray.getBytes(ENCODING);
				new ChunkHeader(dataBlock.getSize(), serializedData.length).writeTo(out);
				compression.encode(ReadData.from(serializedData)).writeTo(out);
				out.flush();
			});
		}

		@Override
		public DataBlock<String[]> decode(final ReadData readData, final long[] gridPosition, final Compression compression) throws IOException {
			try(final InputStream in = readData.inputStream()) {
				final ChunkHeader header = ChunkHeader.readFrom(in, MODE_DEFAULT, MODE_VARLENGTH);
				final ReadData decompressed = compression.decode(ReadData.from(in), header.numElements());
				final byte[] serializedData = decompressed.allBytes();
				final String rawChars = new String(serializedData, ENCODING);
				final String[] actualData = rawChars.split(NULLCHAR);
				return new StringDataBlock(header.blockSize(), gridPosition, actualData);
			}
		}
	}

	/**
	 * TODO javadoc
	 */
	static class ObjectDataBlockCodec implements DataBlockCodec<byte[]> {

		@Override
		public ReadData encode(final DataBlock<byte[]> dataBlock, final Compression compression) throws IOException {
			return ReadData.from(out -> {
				new ChunkHeader(null, dataBlock.getNumElements()).writeTo(out);
				compression.encode(ReadData.from(dataBlock.getData())).writeTo(out);
				out.flush();
			});
		}

		@Override
		public DataBlock<byte[]> decode(final ReadData readData, final long[] gridPosition, final Compression compression) throws IOException {
			try(final InputStream in = readData.inputStream()) {
				final ChunkHeader header = ChunkHeader.readFrom(in, MODE_OBJECT);
				final byte[] data = new byte[header.numElements()];
				final ReadData decompressed = compression.decode(ReadData.from(in), data.length);
				new DataInputStream(decompressed.inputStream()).readFully(data);
				return new ByteArrayDataBlock(null, gridPosition, data);
			}
		}
	}

	static class ChunkHeader {

		public static final short MODE_DEFAULT = 0;
		public static final short MODE_VARLENGTH = 1;
		public static final short MODE_OBJECT = 2;

		private final short mode;
		private final int[] blockSize;
		private final int numElements;

		ChunkHeader(final short mode, final int[] blockSize, final int numElements) {
			this.mode = mode;
			this.blockSize = blockSize;
			this.numElements = numElements;
		}

		ChunkHeader(final int[] blockSize, final int numElements) {
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

		public short mode() {
			return mode;
		}

		public int[] blockSize() {
			return blockSize;
		}

		public int numElements() {
			return numElements;
		}

		private static int[] readBlockSize(final DataInputStream dis) throws IOException {
			final int nDim = dis.readShort();
			final int[] blockSize = new int[nDim];
			for (int d = 0; d < nDim; ++d)
				blockSize[d] = dis.readInt();
			return blockSize;
		}

		private static void writeBlockSize(final int[] blockSize, final DataOutputStream dos) throws IOException {
			dos.writeShort(blockSize.length);
			for (final int size : blockSize)
				dos.writeInt(size);
		}

		void writeTo(final OutputStream out) throws IOException {
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
				throw new IOException("unexpected mode: " + mode);
			}
			dos.flush();
		}

		static ChunkHeader readFrom(final InputStream in) throws IOException {
			return readFrom(in, null);
		}

		static ChunkHeader readFrom(final InputStream in, short... allowedModes) throws IOException {
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
				throw new IOException("unexpected mode: " + mode);
			}

			boolean modeIsOk = allowedModes == null || allowedModes.length == 0;
			for (int i = 0; !modeIsOk && i < allowedModes.length; ++i) {
				if (mode == allowedModes[i])
					modeIsOk = true;
			}
			if (!modeIsOk) {
				throw new IOException("unexpected mode: " + mode);
			}

			return new ChunkHeader(mode, blockSize, numElements);
		}
	}
}
