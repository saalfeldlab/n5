package org.janelia.saalfeldlab.n5.codec.checksum;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import org.junit.Test;

public class Cr32ChecksumCodecTests {

	@Test
	public void testEncodeDecode() throws IOException {

		final int N = 64;
		final Random random = new Random();
		final byte[] data = new byte[N];
		random.nextBytes(data);

		final ByteArrayOutputStream outStream = new ByteArrayOutputStream(N);

		final Crc32cChecksumCodec codec = new Crc32cChecksumCodec();
		codec.encode(outStream);

		final ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
		codec.decode(inStream);


	}

}
