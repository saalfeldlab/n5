/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.NameConfigAdapter;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.BytesCodecTests.BitShiftBytesCodec;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
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
		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(DataCodecInfo.class, NameConfigAdapter.getJsonAdapter(DataCodecInfo.class));
		gsonBuilder.registerTypeHierarchyAdapter(CodecInfo.class, NameConfigAdapter.getJsonAdapter(CodecInfo.class));
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.disableHtmlEscaping();
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

		final DataCodecInfo deserializedCodecInfo = gson.fromJson(bitShiftJson, DataCodecInfo.class);
		// Verify deserialized codec
		assertEquals("Deserialized codec should equal original", codec, deserializedCodecInfo);
	}

	@Test
	public void testSerializeCodecArray() {

		CodecInfo[] codecs = new CodecInfo[]{
				new IdentityCodec()
		};
		JsonArray jsonCodecArray = gson.toJsonTree(codecs).getAsJsonArray();
		JsonElement expected = gson.fromJson(
				"[{\"name\":\"id\"}]",
				JsonElement.class);
		assertEquals("codec array", expected, jsonCodecArray.getAsJsonArray());

		CodecInfo[] codecsDeserialized = gson.fromJson(expected, CodecInfo[].class);
		assertEquals("codecs length not 1", 1, codecsDeserialized.length);
		assertTrue("first codec not identity", codecsDeserialized[0] instanceof IdentityCodec);

		codecs = new CodecInfo[]{
				new GzipCompression()
		};
		jsonCodecArray = gson.toJsonTree(codecs).getAsJsonArray();
		expected = gson.fromJson(
				"[{\"name\":\"gzip\",\"configuration\":{\"level\":-1,\"useZlib\":false}}]",
				JsonElement.class);
		assertEquals("codec array", expected, jsonCodecArray.getAsJsonArray());

		codecsDeserialized = gson.fromJson(expected, CodecInfo[].class);
		assertEquals("codecs length not 1", 1, codecsDeserialized.length);
		assertTrue("second codec not gzip", codecsDeserialized[0] instanceof GzipCompression);
	}

}
