package org.janelia.saalfeldlab.n5.shard;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5FSTest;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.codec.ZarrBlockCodec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.util.GridIterator;
import org.junit.After;
import org.junit.Test;

public class ShardIndexTest {

	private static final N5FSTest tempN5Factory = new N5FSTest();

	@After
	public void removeTempWriters() {
		tempN5Factory.removeTempWriters();
	}

	@Test
	public void testOffsetIndex() throws IOException {

		int[] shardBlockGridSize = new int[]{5,4,3};
		ShardIndex index = new ShardIndex(
				shardBlockGridSize,
				IndexLocation.END, new ZarrBlockCodec());

		GridIterator it = new GridIterator(shardBlockGridSize);
		int i = 0;
		while( it.hasNext()) {
			int j = index.getOffsetIndex(GridIterator.long2int(it.next()));
			assertEquals(i, j);
			i+=2;
		}
		
		
		shardBlockGridSize = new int[]{5,4,3,13};
		index = new ShardIndex(
				shardBlockGridSize,
				IndexLocation.END, new ZarrBlockCodec());

		it = new GridIterator(shardBlockGridSize);
		i = 0;
		while( it.hasNext()) {
			int j = index.getOffsetIndex(GridIterator.long2int(it.next()));
			assertEquals(i, j);
			i+=2;
		}

	}

	@Test
	public void testReadVirtual() throws IOException {

		final N5KeyValueWriter writer = (N5KeyValueWriter) tempN5Factory.createTempN5Writer();
		final KeyValueAccess kva = writer.getKeyValueAccess();

		final int[] shardBlockGridSize = new int[] { 6, 5 };
		final IndexLocation indexLocation = IndexLocation.END;
		final DeterministicSizeCodec[] indexCodecs = new DeterministicSizeCodec[] { new ZarrBlockCodec(),
				new Crc32cChecksumCodec() };

		final String path = Paths.get(Paths.get(writer.getURI()).toAbsolutePath().toString(), "0").toString();

		final ShardIndex index = new ShardIndex(shardBlockGridSize, indexLocation, indexCodecs);
		index.set(0, 6, new int[] { 0, 0 });
		index.set(19, 32, new int[] { 1, 0 });
		index.set(93, 111, new int[] { 3, 0 });
		index.set(143, 1, new int[] { 1, 2 });
		ShardIndex.write(index, kva, path);

		final ShardIndex other = new ShardIndex(shardBlockGridSize, indexLocation, indexCodecs);
		ShardIndex.read(kva, path, other);

		assertEquals(index, other);
	}

	@Test
	public void testReadInMemory() throws IOException {

		final N5KeyValueWriter writer = (N5KeyValueWriter) tempN5Factory.createTempN5Writer();
		final KeyValueAccess kva = writer.getKeyValueAccess();

		final int[] shardBlockGridSize = new int[] { 6, 5 };
		final IndexLocation indexLocation = IndexLocation.END;
		final DeterministicSizeCodec[] indexCodecs = new DeterministicSizeCodec[] { 
				new ZarrBlockCodec(),
				new Crc32cChecksumCodec() };
		final String path = Paths.get(Paths.get(writer.getURI()).toAbsolutePath().toString(), "indexTest").toString();

		final ShardIndex index = new ShardIndex(shardBlockGridSize, indexLocation, indexCodecs);
		index.set(0, 6, new int[] { 0, 0 });
		index.set(19, 32, new int[] { 1, 0 });
		index.set(93, 111, new int[] { 3, 0 });
		index.set(143, 1, new int[] { 1, 2 });
		ShardIndex.write(index, kva, path);

		final ShardIndex indexRead = new ShardIndex(shardBlockGridSize, indexLocation, indexCodecs);
		ShardIndex.read(rawBytes(kva, path), indexRead);

		assertEquals(index, indexRead);
	}

	private static byte[] rawBytes(KeyValueAccess kva, String path) throws IOException {

		final byte[] rawBytes = new byte[(int) kva.size(path)];
		try (final LockedChannel lockedChannel = kva.lockForReading(path)) {
			try (final InputStream is = lockedChannel.newInputStream()) {
				is.read(rawBytes);
			}
		}
		return rawBytes;
	}
}
