package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5FSTest;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.codec.IndexCodecAdapter;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.util.GridIterator;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class ShardIndexTest {

	private static final N5FSTest tempN5Factory = new N5FSTest();

	@After
	public void removeTempWriters() {

		tempN5Factory.removeTempWriters();
	}

	@Test
	@Ignore
	public void testOffsetIndex() {

		// TODO 
//		int[] shardBlockGridSize = new int[]{5, 4, 3};
//		ShardIndex index = new ShardIndex(
//				shardBlockGridSize,
//				IndexLocation.END, new RawBlockCodecInfo());
//
//		GridIterator it = new GridIterator(shardBlockGridSize);
//		int i = 0;
//		while (it.hasNext()) {
//			int j = index.getOffsetIndex(GridIterator.long2int(it.next()));
//			assertEquals(i, j);
//			i += 2;
//		}
//
//		shardBlockGridSize = new int[]{5, 4, 3, 13};
//		index = new ShardIndex(
//				shardBlockGridSize,
//				IndexLocation.END, new RawBlockCodecInfo());
//
//		it = new GridIterator(shardBlockGridSize);
//		i = 0;
//		while (it.hasNext()) {
//			int j = index.getOffsetIndex(GridIterator.long2int(it.next()));
//			assertEquals(i, j);
//			i += 2;
//		}

	}

	@Test
	@Ignore
	public void writeReadTest() throws IOException {
		
		// TODO

//		final N5KeyValueWriter writer = (N5KeyValueWriter)tempN5Factory.createTempN5Writer();
//		final KeyValueAccess kva = writer.getKeyValueAccess();
//
//		final int[] shardBlockGridSize = new int[]{6, 5};
//		final IndexLocation indexLocation = IndexLocation.END;
//		final IndexCodecAdapter indexCodecAdapter = new IndexCodecAdapter(
//				new RawBlockCodecInfo(),
//				new Crc32cChecksumCodec()
//		);
//
//		final ShardIndex index = new ShardIndex(shardBlockGridSize, indexLocation, indexCodecAdapter);
//		index.set(0, 6, new int[]{0, 0});
//		index.set(19, 32, new int[]{1, 0});
//		index.set(93, 111, new int[]{3, 0});
//		index.set(143, 1, new int[]{1, 2});
//
//		final String path = Paths.get(Paths.get(writer.getURI()).toAbsolutePath().toString(), "indexTest").toString();
//		try (
//				final LockedChannel channel = kva.lockForWriting(path);
//				final OutputStream out = channel.newOutputStream()
//		) {
//
//			ShardIndex.write(out, index);
//		}
//
//		final ShardIndex indexRead = new ShardIndex(shardBlockGridSize, indexLocation, indexCodecAdapter);
//		try (
//				final LockedChannel channel = kva.lockForReading(path);
//				final InputStream in = channel.newInputStream()
//		) {
//			ShardIndex.read(in, indexRead);
//		}
//		assertEquals(index, indexRead);
	}
}
