package org.janelia.saalfeldlab.n5.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteOrder;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.NameConfigAdapter;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.universe.N5Factory;
import org.junit.Test;

import com.google.gson.GsonBuilder;

public class BytesTests {

	@Test
	public void testSerialization() {

		final N5Factory factory = new N5Factory();
		factory.cacheAttributes(false);
		final GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeHierarchyAdapter(Codec.class, NameConfigAdapter.getJsonAdapter(Codec.class));
		gsonBuilder.registerTypeAdapter(ByteOrder.class, BytesCodec.byteOrderAdapter);
		factory.gsonBuilder(gsonBuilder);

		final N5Writer reader = factory.openWriter("n5:src/test/resources/shardExamples/test.zarr");
		final Codec bytes = reader.getAttribute("mid_sharded", "codecs[0]/configuration/codecs[0]", Codec.class);
		assertTrue("as BytesCodec", bytes instanceof N5BlockCodec);

		final N5Writer writer = factory.openWriter("n5:src/test/resources/shardExamples/test.n5");

		final DatasetAttributes datasetAttributes = new DatasetAttributes(
				new long[]{8, 8},
				new int[]{4, 4},
				DataType.UINT8,
				new RawCompression(),
				new Codec[]{
						new N5BlockCodec(ByteOrder.LITTLE_ENDIAN),
						new IdentityCodec()
				}
		);
		writer.setAttribute("shard", "/", datasetAttributes);
		final DatasetAttributes deserialized = writer.getAttribute("shard", "/", DatasetAttributes.class);

		assertEquals("1 codecs", 1, deserialized.getCodecs().length);
		assertTrue("Identity", deserialized.getCodecs()[0] instanceof IdentityCodec);
		assertTrue("Bytes", deserialized.getArrayCodec() instanceof N5BlockCodec);
		assertEquals("LittleEndian", ByteOrder.LITTLE_ENDIAN,
				((N5BlockCodec)deserialized.getArrayCodec()).byteOrder);
	}
}
