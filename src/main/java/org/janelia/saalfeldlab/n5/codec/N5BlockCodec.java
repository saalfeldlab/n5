package org.janelia.saalfeldlab.n5.codec;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;

import org.checkerframework.checker.units.qual.N;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

@NameConfig.Name(value = N5BlockCodec.TYPE)
public class N5BlockCodec implements Codec.ArrayCodec {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String TYPE = "n5bytes";

	@Nullable
	@NameConfig.Parameter(value = "endian", optional = true)
	protected final ByteOrder byteOrder;

	public N5BlockCodec() {

		this(ByteOrder.BIG_ENDIAN);
	}

	public N5BlockCodec(@Nullable final ByteOrder byteOrder) {

		this.byteOrder = byteOrder;
	}

	/**
	 * ByteOrder used to encode/decode this block of data.<br>
	 * Will be `null` when {@link DatasetAttributes#getDataType()} refers to a single-byte type,
	 *
	 * @return the byte order for this codec
	 */
	@CheckForNull
	public ByteOrder getByteOrder() {
		return byteOrder;
	}

	@Override public DataBlockInputStream decode(final DatasetAttributes attributes, final long[] gridPosition, InputStream in) throws IOException {

		return new N5DataBlockInputStream(in, attributes, gridPosition, byteOrder);
	}


	@Override
	public DataBlockOutputStream encode(final DatasetAttributes attributes, final DataBlock<?> dataBlock,
			final OutputStream out)
			throws IOException {

		return new N5DataBlockOutputStream(out, attributes, dataBlock, byteOrder);
	}

	@Override
	public String getType() {

		return TYPE;
	}

	private static class N5DataBlockOutputStream extends DataBlockOutputStream {

		private final DatasetAttributes attributes;
		private final DataBlock<?> dataBlock;
		private final ByteOrder byteOrder;
		boolean start = true;


		public N5DataBlockOutputStream(final OutputStream out, final DatasetAttributes attributes, final DataBlock<?> dataBlock, ByteOrder byteOrder) {
			super(out);
			this.attributes = attributes;
			this.dataBlock = dataBlock;
			this.byteOrder = byteOrder;
		}

		@Override
		protected void beforeWrite(int n) throws IOException {

			if (start) {
				writeHeader();
				start = false;
			}
		}

		private void writeHeader() throws IOException {
			final DataOutput dos = getDataOutput(out);

			final int mode;
			if (attributes.getDataType() == DataType.OBJECT || dataBlock.getSize() == null)
				mode = 2;
			else if (dataBlock.getNumElements() == DataBlock.getNumElements(dataBlock.getSize()))
				mode = 0;
			else
				mode = 1;
			dos.writeShort(mode);

			if (mode != 2) {
				dos.writeShort(attributes.getNumDimensions());
				for (final int size : dataBlock.getSize())
					dos.writeInt(size);
			}

			if (mode != 0)
				dos.writeInt(dataBlock.getNumElements());
		}

		@Override
		public DataOutput getDataOutput(final OutputStream outputStream) {

			if (byteOrder.equals(ByteOrder.BIG_ENDIAN))
				return new DataOutputStream(outputStream);
			else
				return new LittleEndianDataOutputStream(outputStream);
		}
	}

	private static class N5DataBlockInputStream extends DataBlockInputStream {
		private final DatasetAttributes attributes;
		private final long[] gridPosition;
		private final ByteOrder byteOrder;

		private short mode = -1;
		private int[] blockSize = null;
		private int numElements = -1;

		private boolean start = true;

		N5DataBlockInputStream(final InputStream in, final DatasetAttributes attributes, final long[] gridPosition, ByteOrder byteOrder) {
			super(in);
			this.attributes = attributes;
			this.gridPosition = gridPosition;
			this.byteOrder = byteOrder;
		}

		@Override protected void beforeRead(int n) throws IOException {

			if (start) {
				readHeader();
				start = false;
			}
		}

		@Override
		public DataBlock<?> allocateDataBlock() throws IOException {
			if (start) {
				readHeader();
				start = false;
			}
			if (mode != 2) {
				return attributes.getDataType().createDataBlock(blockSize, gridPosition, numElements);
			} else {
				return attributes.getDataType().createDataBlock(null, gridPosition, numElements);
			}
		}

		private void readHeader() throws IOException {
			final DataInput dis = getDataInput(in);
			mode = dis.readShort();
			if (mode != 2) {
				final int nDim = dis.readShort();
				blockSize = new int[nDim];
				for (int d = 0; d < nDim; ++d)
					blockSize[d] = dis.readInt();
				if (mode == 0) {
					numElements = DataBlock.getNumElements(blockSize);
				} else {
					numElements = dis.readInt();
				}

			} else {
				numElements = dis.readInt();
			}
		}

		@Override
		public DataInput getDataInput(final InputStream inputStream) {

			if (byteOrder.equals(ByteOrder.BIG_ENDIAN))
				return new DataInputStream(inputStream);
			else
				return new LittleEndianDataInputStream(inputStream);
		}

	}

}
