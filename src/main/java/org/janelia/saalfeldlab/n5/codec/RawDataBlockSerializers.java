package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.StringDataBlock;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import java.nio.ByteOrder;

public class RawDataBlockSerializers {

	private static final DataBlockSerializerFactory<byte[]>   BYTE   = (byteOrder, blockSize, codec) -> new RawDataBlockSerializer<>(FlatArraySerializer.BYTE, ByteArrayDataBlock::new, blockSize, codec);
	private static final DataBlockSerializerFactory<short[]>  SHORT  = (byteOrder, blockSize, codec) -> new RawDataBlockSerializer<>(FlatArraySerializer.SHORT(byteOrder), ShortArrayDataBlock::new, blockSize, codec);
	private static final DataBlockSerializerFactory<int[]>    INT    = (byteOrder, blockSize, codec) -> new RawDataBlockSerializer<>(FlatArraySerializer.INT(byteOrder), IntArrayDataBlock::new, blockSize, codec);
	private static final DataBlockSerializerFactory<long[]>   LONG   = (byteOrder, blockSize, codec) -> new RawDataBlockSerializer<>(FlatArraySerializer.LONG(byteOrder), LongArrayDataBlock::new, blockSize, codec);
	private static final DataBlockSerializerFactory<float[]>  FLOAT  = (byteOrder, blockSize, codec) -> new RawDataBlockSerializer<>(FlatArraySerializer.FLOAT(byteOrder), FloatArrayDataBlock::new, blockSize, codec);
	private static final DataBlockSerializerFactory<double[]> DOUBLE = (byteOrder, blockSize, codec) -> new RawDataBlockSerializer<>(FlatArraySerializer.DOUBLE(byteOrder), DoubleArrayDataBlock::new, blockSize, codec);
	private static final DataBlockSerializerFactory<String[]> STRING = (byteOrder, blockSize, codec) -> new RawDataBlockSerializer<>(FlatArraySerializer.ZARR_STRING, StringDataBlock::new,  blockSize, codec);
	private static final DataBlockSerializerFactory<byte[]>   OBJECT = (byteOrder, blockSize, codec) -> new RawDataBlockSerializer<>(FlatArraySerializer.OBJECT, ByteArrayDataBlock::new, blockSize, codec);

	private RawDataBlockSerializers() {}

	public static <T> DataBlockSerializer<T> create(
			final DataType dataType,
			final ByteOrder byteOrder,
			final int[] blockSize,
			final BytesCodec codec) {
		final DataBlockSerializerFactory<?> factory;
		switch (dataType) {
		case UINT8:
		case INT8:
			factory = RawDataBlockSerializers.BYTE;
			break;
		case UINT16:
		case INT16:
			factory = RawDataBlockSerializers.SHORT;
			break;
		case UINT32:
		case INT32:
			factory = RawDataBlockSerializers.INT;
			break;
		case UINT64:
		case INT64:
			factory = RawDataBlockSerializers.LONG;
			break;
		case FLOAT32:
			factory = RawDataBlockSerializers.FLOAT;
			break;
		case FLOAT64:
			factory = RawDataBlockSerializers.DOUBLE;
			break;
		case STRING:
			factory = RawDataBlockSerializers.STRING;
			break;
		// TODO: What about OBJECT?
		default:
			throw new IllegalArgumentException("Unsupported data type: " + dataType);
		}
		final DataBlockSerializerFactory<T> tFactory = (DataBlockSerializerFactory<T>) factory;
		return tFactory.create(byteOrder, blockSize, codec);
	}

	private interface DataBlockSerializerFactory<T> {

		/**
		 * Create a {@link DataBlockSerializer} that uses the specified {@code
		 * ByteOrder} and {@code BytesCodes} and de/serializes {@code
		 * DataBlock<T>} of the specified {@code blockSize} to raw format.
		 *
		 * @return Raw {@code DataBlockSerializer}
		 */
		DataBlockSerializer<T> create(ByteOrder byteOrder, int[] blockSize, BytesCodec codec);
	}

	private static class RawDataBlockSerializer<T> implements DataBlockSerializer<T> {

		private final FlatArraySerializer<T> dataCodec;
		private final DataBlock.DataBlockFactory<T> dataBlockFactory;
		private final int[] blockSize;
		private final int numElements;
		private final BytesCodec codec;

		RawDataBlockSerializer(
				final FlatArraySerializer<T> dataCodec,
				final DataBlock.DataBlockFactory<T> dataBlockFactory,
				final int[] blockSize,
				final BytesCodec codec) {

			this.dataCodec = dataCodec;
			this.dataBlockFactory = dataBlockFactory;
			this.blockSize = blockSize;
			this.numElements = DataBlock.getNumElements(blockSize);
			this.codec = codec;
		}

		@Override
		public ReadData encode(DataBlock<T> dataBlock) {

			return ReadData.from(out -> {
				final ReadData blockData = dataCodec.serialize(dataBlock.getData());
				codec.encode(blockData).writeTo(out);
			});
		}

		@Override
		public DataBlock<T> decode(ReadData readData, long[] gridPosition) {

			final ReadData decodeData = codec.decode(readData);
			final T data = dataCodec.deserialize(decodeData, numElements);
			return dataBlockFactory.createDataBlock(blockSize, gridPosition, data);
		}
	}
}
