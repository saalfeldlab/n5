package org.janelia.saalfeldlab.n5.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.junit.Test;

public class ChecksumCodecTests {

	@Test
	public void testCrc32cChecksumCodec() {
		
		final ReadData rd = ReadData.from(new byte[] {0,1,2,3,4,5,6,7,8,9});
		final long N = rd.requireLength();

		final Crc32cChecksumCodec codec = new  Crc32cChecksumCodec();
		final ReadData encoded = codec.encode(rd);

		// Crc32 adds 4 bytes to the data
		assertEquals(N+codec.numChecksumBytes(), encoded.requireLength());

		final ReadData decoded = codec.decode(encoded);
		assertArrayEquals(rd.allBytes(), decoded.allBytes());

		// attempting to decode perturbed data throws exception
		final byte[] encodedBytes = encoded.allBytes();
		encodedBytes[1]++;
		final ReadData perturbed = ReadData.from(encodedBytes);
		assertThrows(N5Exception.class, () -> codec.decode(perturbed));
	}

}
