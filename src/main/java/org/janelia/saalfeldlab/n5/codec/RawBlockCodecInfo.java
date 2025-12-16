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
package org.janelia.saalfeldlab.n5.codec;

import java.nio.ByteOrder;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name(value = RawBlockCodecInfo.TYPE)
public class RawBlockCodecInfo implements BlockCodecInfo {

	private static final long serialVersionUID = 3282569607795127005L;

	public static final String TYPE = "raw-bytes";

	@NameConfig.Parameter(value = "endian", optional = true)
	private final ByteOrder byteOrder;

	public RawBlockCodecInfo() {

		this(ByteOrder.BIG_ENDIAN);
	}

	public RawBlockCodecInfo(final ByteOrder byteOrder) {

		this.byteOrder = byteOrder;
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public ByteOrder getByteOrder() {
		return byteOrder;
	}

	@Override
	public <T> BlockCodec<T> create(final DataType dataType, final int[] blockSize, final DataCodecInfo... codecInfos) {
		ensureValidByteOrder(dataType, getByteOrder());
		return RawBlockCodecs.create(dataType, byteOrder, blockSize, DataCodec.create(codecInfos));
	}

	public static void ensureValidByteOrder(final DataType dataType, final ByteOrder byteOrder) {

		switch (dataType) {
		case INT8:
		case UINT8:
		case STRING:
		case OBJECT:
			return;
		}

		if (byteOrder == null)
			throw new IllegalArgumentException("DataType (" + dataType + ") requires ByteOrder, but was null");
	}

	public static ByteOrderAdapter byteOrderAdapter = new ByteOrderAdapter();

	public static class ByteOrderAdapter implements JsonDeserializer<ByteOrder>, JsonSerializer<ByteOrder> {

		@Override
		public JsonElement serialize(ByteOrder src, java.lang.reflect.Type typeOfSrc,
				JsonSerializationContext context) {

			if (src.equals(ByteOrder.LITTLE_ENDIAN))
				return new JsonPrimitive("little");
			else
				return new JsonPrimitive("big");
		}

		@Override
		public ByteOrder deserialize(JsonElement json, java.lang.reflect.Type typeOfT,
				JsonDeserializationContext context) throws JsonParseException {

			if (json.getAsString().equals("little"))
				return ByteOrder.LITTLE_ENDIAN;
			if (json.getAsString().equals("big"))
				return ByteOrder.BIG_ENDIAN;

			return null;
		}

	}
}
