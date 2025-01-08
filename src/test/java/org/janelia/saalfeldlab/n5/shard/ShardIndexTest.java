package org.janelia.saalfeldlab.n5.shard;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5FSTest;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

public class ShardIndexTest {

	private static final N5FSTest tempN5Factory = new N5FSTest();

	@After
	public void removeTempWriters() {
		tempN5Factory.removeTempWriters();
	}

	@Test
	public void testReadVirtual() throws IOException {

		final N5KeyValueWriter writer = (N5KeyValueWriter) tempN5Factory.createTempN5Writer();
		final KeyValueAccess kva = writer.getKeyValueAccess();

		final int[] shardBlockGridSize = new int[] { 6, 5 };
		final IndexLocation indexLocation = IndexLocation.END;
		final DeterministicSizeCodec[] indexCodecs = new DeterministicSizeCodec[] { new BytesCodec(),
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
	@Ignore
	public void testReadInMemory() throws IOException {

		final N5KeyValueWriter writer = (N5KeyValueWriter) tempN5Factory.createTempN5Writer();
		final KeyValueAccess kva = writer.getKeyValueAccess();

		final int[] shardBlockGridSize = new int[] { 6, 5 };
		final IndexLocation indexLocation = IndexLocation.END;
		final DeterministicSizeCodec[] indexCodecs = new DeterministicSizeCodec[] { new BytesCodec(),
				new Crc32cChecksumCodec() };
		final String path = Paths.get(Paths.get(writer.getURI()).toAbsolutePath().toString(), "0").toString();

		final ShardIndex index = new ShardIndex(shardBlockGridSize, indexLocation, indexCodecs);
		index.set(0, 6, new int[] { 0, 0 });
		index.set(19, 32, new int[] { 1, 0 });
		index.set(93, 111, new int[] { 3, 0 });
		index.set(143, 1, new int[] { 1, 2 });
		ShardIndex.write(index, kva, path);

		ShardedDatasetAttributes attrs = new ShardedDatasetAttributes(
				new long[]{6,5},
				shardBlockGridSize,
				new int[]{1,1},
				DataType.UINT8,
				new Codec[]{new N5BlockCodec(), new GzipCompression(4)},
				new DeterministicSizeCodec[]{new BytesCodec(), new Crc32cChecksumCodec()},
				indexLocation
		);

		final InMemoryShard shard = InMemoryShard.readShard(kva, path, new long[] {0,0}, attrs);

		assertEquals(index, shard.index);
	}
}
