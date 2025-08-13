package org.janelia.saalfeldlab.n5.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.codec.ArrayCodec;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.BytesCodecTests.BitShiftBytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
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
		GsonUtils.registerGson(gsonBuilder);
		gson = gsonBuilder.create();
	}

	@Test
	public void testCodecSerialization() {

		final IdentityCodec id = new IdentityCodec();
		final JsonObject jsonId = gson.toJsonTree(id).getAsJsonObject();
		final JsonElement expected = gson.fromJson("{\"name\":\"id\"}", JsonElement.class);
		assertEquals("identity json", expected, jsonId.getAsJsonObject());

		final BitShiftBytesCodec codec = new BitShiftBytesCodec(3);
		final JsonObject bitShiftJson = gson.toJsonTree(codec).getAsJsonObject();
		final JsonElement expectedBitShift = gson.fromJson(
				"{\"name\":\"bitshift\",\"configuration\":{\"shift\":3}}",
				JsonElement.class);
		assertEquals("bitshift json", expectedBitShift, bitShiftJson);

		final BytesCodec deserializedCodec = gson.fromJson(bitShiftJson, BytesCodec.class);
		// Verify deserialized codec
		assertEquals("Deserialized codec should equal original", codec, deserializedCodec);
	}

	@Test
	public void testSerializeCodecArray() {

		Codec[] codecs = new Codec[]{
				new IdentityCodec()
		};
		JsonArray jsonCodecArray = gson.toJsonTree(codecs).getAsJsonArray();
		JsonElement expected = gson.fromJson(
				"[{\"name\":\"id\"}]",
				JsonElement.class);
		assertEquals("codec array", expected, jsonCodecArray.getAsJsonArray());

		Codec[] codecsDeserialized = gson.fromJson(expected, Codec[].class);
		assertEquals("codecs length not 1", 1, codecsDeserialized.length);
		assertTrue("first codec not identity", codecsDeserialized[0] instanceof IdentityCodec);

		codecs = new Codec[]{
				new GzipCompression()
		};
		jsonCodecArray = gson.toJsonTree(codecs).getAsJsonArray();
		expected = gson.fromJson(
				"[{\"name\":\"gzip\",\"configuration\":{\"level\":-1,\"useZlib\":false}}]",
				JsonElement.class);
		assertEquals("codec array", expected, jsonCodecArray.getAsJsonArray());

		codecsDeserialized = gson.fromJson(expected, Codec[].class);
		assertEquals("codecs length not 1", 1, codecsDeserialized.length);
		assertTrue("second codec not gzip", codecsDeserialized[0] instanceof GzipCompression);
	}

}
