package org.janelia.saalfeldlab.n5.codec.checksum;

import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

@NameConfig.Name(Crc32cChecksumCodec.TYPE)
public class Crc32cChecksumCodec extends ChecksumCodec {

	private static final long serialVersionUID = 7424151868725442500L;

	public static final String TYPE = "crc32c";

	public Crc32cChecksumCodec() {

		super(new CRC32(), 4);
	}

	@Override
	public long encodedSize(final long size) {

		return size + numChecksumBytes();
	}

	@Override
	public long decodedSize(final long size) {

		return size - numChecksumBytes();
	}

	@Override
	public ByteBuffer getChecksumValue() {

		final ByteBuffer buf = ByteBuffer.allocate(numChecksumBytes());
		buf.putInt((int)getChecksum().getValue());
		return buf;
	}

	@Override
	public String getType() {

		return TYPE;
	}

}
