/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import java.lang.reflect.Type;

import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.FixedScaleOffsetCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingConfiguration;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class CodecAdapter implements JsonDeserializer<Codec>, JsonSerializer<Codec> {

	@Override
	public JsonElement serialize(
			final Codec codec,
			final Type typeOfSrc,
			final JsonSerializationContext context) {

		if (codec.getType().equals(FixedScaleOffsetCodec.TYPE)) {
			final FixedScaleOffsetCodec c = (FixedScaleOffsetCodec)codec;
			final JsonObject obj = new JsonObject();
			obj.addProperty("name", c.getType());
			obj.addProperty("scale", c.getScale());
			obj.addProperty("offset", c.getOffset());
			obj.addProperty("type", c.getType().toString().toLowerCase());
			obj.addProperty("encodedType", c.getEncodedDataType().toString().toLowerCase());
			return obj;
		}
		else if (codec.getType().equals(ShardingCodec.TYPE)) {
			final ShardingCodec sharding = (ShardingCodec)codec;
			final JsonObject obj = new JsonObject();
			obj.addProperty("name", sharding.getType());
			obj.add("configuration", context.serialize(sharding.getConfiguration()));
			return obj;
		}
		else if (codec.getType().equals(BytesCodec.TYPE)) {
			final BytesCodec bytes = (BytesCodec)codec;
			final JsonObject obj = new JsonObject();
			obj.addProperty("type", bytes.getType());

			final JsonObject config = new JsonObject();
			config.addProperty("endian", bytes.getType());
			obj.add("configuration", config);

			return obj;
		}

		return JsonNull.INSTANCE;
	}

	@Override
	public Codec deserialize(
			final JsonElement json,
			final Type typeOfT,
			final JsonDeserializationContext context) throws JsonParseException {

		if (json == null)
			return null;
		else if (!json.isJsonObject())
			return null;

		final JsonObject jsonObject = json.getAsJsonObject();
		if (jsonObject.has("type")) {

			final String type = jsonObject.get("type").getAsString();
			if (type.equals(FixedScaleOffsetCodec.TYPE)) {

				return new FixedScaleOffsetCodec(
						jsonObject.get("scale").getAsDouble(),
						jsonObject.get("offset").getAsDouble(),
						DataType.valueOf(jsonObject.get("type").getAsString().toUpperCase()),
						DataType.valueOf(jsonObject.get("encodedType").getAsString().toUpperCase()));
			}
			else if (type.equals(ShardingCodec.TYPE)) {
				return new ShardingCodec(
						context.deserialize(jsonObject.get("configuration"), ShardingConfiguration.class));
			} else if (type.equals(BytesCodec.TYPE)) {

				// TODO implement
				return new BytesCodec();
			}
		}

		return null;
	}

}