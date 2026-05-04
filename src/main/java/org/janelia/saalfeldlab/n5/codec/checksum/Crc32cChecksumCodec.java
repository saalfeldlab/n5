package org.janelia.saalfeldlab.n5.codec.checksum;

import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.Checksum;

@NameConfig.Name(Crc32cChecksumCodec.TYPE)
public class Crc32cChecksumCodec extends ChecksumCodec {

	private static final long serialVersionUID = 7424151868725442500L;

	public static final String TYPE = "crc32c";

	public Crc32cChecksumCodec() {

		super(() -> new PureJavaCrc32C(), 4);
	}

	@Override
	public ByteBuffer getChecksumValue(Checksum checksum) {

		final ByteBuffer buf = ByteBuffer.allocate(numChecksumBytes());
		buf.order(ByteOrder.LITTLE_ENDIAN).putInt((int)checksum.getValue());
		return buf;
	}

	@Override
	public String getType() {

		return TYPE;
	}

	@Override public DataCodec create() {

		return this;
	}

}
