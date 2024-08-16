package org.janelia.saalfeldlab.n5.codec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.commons.io.output.ProxyOutputStream;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name(value = BytesCodec.TYPE)
public class BytesCodec implements Codec.ArrayToBytes {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String TYPE = "bytes";

	@NameConfig.Parameter(value = "endian", optional = true)
	protected final ByteOrder byteOrder;

	public BytesCodec() {

		this(ByteOrder.LITTLE_ENDIAN);
	}

	public BytesCodec(final ByteOrder byteOrder) {

		this.byteOrder = byteOrder;

	}

	@Override public DataBlockInputStream decode(final DatasetAttributes attributes, final long[] gridPosition, InputStream in) throws IOException {

		return new DataBlockInputStream(in) {

			private short mode = -1;
			private int[] blockSize = null;
			private int numElements = -1;

			private boolean start = true;

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
				final DataInputStream dis = new DataInputStream(in);
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
		};
	}

	@Override public OutputStream encode(final DatasetAttributes attributes, final DataBlock<?> dataBlock, final OutputStream out) throws IOException {

		return new ProxyOutputStream(out) {

			boolean start = true;

			@Override protected void beforeWrite(int n) throws IOException {
				if (start) {
					writeHeader();
					start = false;
				}
			}

			private void writeHeader() throws IOException {
				final DataOutputStream dos = new DataOutputStream(out);

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
		};
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public static final ByteOrderAdapter byteOrderAdapter = new ByteOrderAdapter();

	public static class ByteOrderAdapter implements JsonDeserializer<ByteOrder>, JsonSerializer<ByteOrder> {

		@Override
		public JsonElement serialize(ByteOrder src, java.lang.reflect.Type typeOfSrc,
				JsonSerializationContext context) {

			if (src.equals(ByteOrder.LITTLE_ENDIAN))
				return new JsonPrimitive("little");
			else
				return new JsonPrimitive("big");
		}

		@Override
		public ByteOrder deserialize(JsonElement json, java.lang.reflect.Type typeOfT,
				JsonDeserializationContext context) throws JsonParseException {

			if (json.getAsString().equals("little"))
				return ByteOrder.LITTLE_ENDIAN;
			if (json.getAsString().equals("big"))
				return ByteOrder.BIG_ENDIAN;

			return null;
		}

	}
}
