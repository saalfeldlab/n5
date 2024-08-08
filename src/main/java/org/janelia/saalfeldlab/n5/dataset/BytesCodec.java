package org.janelia.saalfeldlab.n5.dataset;

import java.nio.ByteOrder;

import org.janelia.saalfeldlab.n5.serialization.N5NameConfig;

@N5NameConfig.Type("bytes")
@N5NameConfig.Prefix("codec")
public class BytesCodec implements DatasetToByteStream {

	private static final long serialVersionUID = 3523505403978222360L;

	protected final ByteOrder byteOrder;

	protected transient final byte[] array;

	public BytesCodec() {

		this(ByteOrder.LITTLE_ENDIAN);
	}

	public BytesCodec(final ByteOrder byteOrder) {

		this(byteOrder, 256);
	}

	public BytesCodec(final ByteOrder byteOrder, final int N) {

		this.byteOrder = byteOrder;
		this.array = new byte[N];
	}

}
