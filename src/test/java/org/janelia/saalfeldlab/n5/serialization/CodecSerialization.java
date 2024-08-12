package org.janelia.saalfeldlab.n5.serialization;

import static org.junit.Assert.assertEquals;

import org.janelia.saalfeldlab.n5.DataType;
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

		final GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapter(IdentityCodec.class, NameConfigAdapter.getJsonAdapter(IdentityCodec.class));
		builder.registerTypeAdapter(AsTypeCodec.class, NameConfigAdapter.getJsonAdapter(AsTypeCodec.class));
		builder.registerTypeAdapter(FixedScaleOffsetCodec.class,
				NameConfigAdapter.getJsonAdapter(FixedScaleOffsetCodec.class));
		gson = builder.create();
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

		final Codec[] codecs = new Codec[]{
				new IdentityCodec(),
				new AsTypeCodec(DataType.FLOAT64, DataType.INT16)
		};
		final JsonArray jsonCodecArray = gson.toJsonTree(codecs).getAsJsonArray();
		final JsonElement expected = gson.fromJson(
				"[{\"name\":\"id\",\"configuration\":{}},{\"name\":\"astype\",\"configuration\":{\"dataType\":\"FLOAT64\",\"encodedType\":\"INT16\"}}]",
				JsonElement.class);

		assertEquals("codec array", expected, jsonCodecArray.getAsJsonArray());
	}

}
