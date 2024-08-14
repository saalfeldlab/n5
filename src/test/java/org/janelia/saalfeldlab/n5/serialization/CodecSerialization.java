package org.janelia.saalfeldlab.n5.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.NameConfigAdapter;
import org.janelia.saalfeldlab.n5.codec.AsTypeCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.FixedScaleOffsetCodec;
import org.janelia.saalfeldlab.n5.codec.IdentityCodec;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class CodecSerialization {

	private Gson gson;

	@Before
	public void before() {

		final GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(IdentityCodec.class, NameConfigAdapter.getJsonAdapter(IdentityCodec.class));
		gsonBuilder.registerTypeAdapter(AsTypeCodec.class, NameConfigAdapter.getJsonAdapter(AsTypeCodec.class));
		gsonBuilder.registerTypeAdapter(FixedScaleOffsetCodec.class,
				NameConfigAdapter.getJsonAdapter(FixedScaleOffsetCodec.class));
		gsonBuilder.registerTypeAdapter(GzipCompression.class,
				NameConfigAdapter.getJsonAdapter(GzipCompression.class));
		gsonBuilder.registerTypeAdapter(Codec.class,
				NameConfigAdapter.getJsonAdapter(Codec.class));

		gson = gsonBuilder.create();
	}

	@Test
	public void testSerializeIdentity() {

		final IdentityCodec id = new IdentityCodec();
		final JsonObject jsonId = gson.toJsonTree(id).getAsJsonObject();
		final JsonElement expected = gson.fromJson("{\"name\":\"id\", \"configuration\":{}}", JsonElement.class);
		assertEquals("identity", expected, jsonId.getAsJsonObject());
	}

	@Test
	public void testSerializeAsType() {

		final AsTypeCodec asTypeCodec = new AsTypeCodec(DataType.FLOAT64, DataType.INT16);
		final JsonObject jsonAsType = gson.toJsonTree(asTypeCodec).getAsJsonObject();
		final JsonElement expected = gson.fromJson(
				"{\"name\":\"astype\",\"configuration\":{\"dataType\":\"FLOAT64\",\"encodedType\":\"INT16\"}}",
				JsonElement.class);
		assertEquals("asType", expected, jsonAsType.getAsJsonObject());
	}

	@Test
	public void testSerializeCodecArray() {

		Codec[] codecs = new Codec[]{
				new IdentityCodec(),
				new AsTypeCodec(DataType.FLOAT64, DataType.INT16)
		};
		JsonArray jsonCodecArray = gson.toJsonTree(codecs).getAsJsonArray();
		JsonElement expected = gson.fromJson(
				"[{\"name\":\"id\",\"configuration\":{}},{\"name\":\"astype\",\"configuration\":{\"dataType\":\"FLOAT64\",\"encodedType\":\"INT16\"}}]",
				JsonElement.class);
		assertEquals("codec array", expected, jsonCodecArray.getAsJsonArray());

		Codec[] codecsDeserialized = gson.fromJson(expected, Codec[].class);
		assertEquals("codecs length not 2", 2, codecsDeserialized.length);
		assertTrue("first codec not identity", codecsDeserialized[0] instanceof IdentityCodec);
		assertTrue("second codec not asType", codecsDeserialized[1] instanceof AsTypeCodec);

		codecs = new Codec[]{
				new AsTypeCodec(DataType.FLOAT64, DataType.INT16),
				new GzipCompression()
		};
		jsonCodecArray = gson.toJsonTree(codecs).getAsJsonArray();
		expected = gson.fromJson(
				"[{\"name\":\"astype\",\"configuration\":{\"dataType\":\"FLOAT64\",\"encodedType\":\"INT16\"}},{\"name\":\"gzip\",\"configuration\":{\"level\":-1,\"use_z_lib\":false}}]",
				JsonElement.class);
		assertEquals("codec array", expected, jsonCodecArray.getAsJsonArray());

		codecsDeserialized = gson.fromJson(expected, Codec[].class);
		assertEquals("codecs length not 2", 2, codecsDeserialized.length);
		assertTrue("first codec not asType", codecsDeserialized[0] instanceof AsTypeCodec);
		assertTrue("second codec not gzip", codecsDeserialized[1] instanceof GzipCompression);
	}

}
