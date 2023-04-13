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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Enumerates available data types.
 *
 * @author Stephan Saalfeld
 */
public enum DataType {

	UINT8("uint8", (blockSize, gridPosition, numElements) -> new ByteArrayDataBlock(blockSize, gridPosition, new byte[numElements])),
	UINT16("uint16", (blockSize, gridPosition, numElements) -> new ShortArrayDataBlock(blockSize, gridPosition, new short[numElements])),
	UINT32("uint32", (blockSize, gridPosition, numElements) -> new IntArrayDataBlock(blockSize, gridPosition, new int[numElements])),
	UINT64("uint64", (blockSize, gridPosition, numElements) -> new LongArrayDataBlock(blockSize, gridPosition, new long[numElements])),
	INT8("int8", (blockSize, gridPosition, numElements) -> new ByteArrayDataBlock(blockSize, gridPosition, new byte[numElements])),
	INT16("int16", (blockSize, gridPosition, numElements) -> new ShortArrayDataBlock(blockSize, gridPosition, new short[numElements])),
	INT32("int32", (blockSize, gridPosition, numElements) -> new IntArrayDataBlock(blockSize, gridPosition, new int[numElements])),
	INT64("int64", (blockSize, gridPosition, numElements) -> new LongArrayDataBlock(blockSize, gridPosition, new long[numElements])),
	FLOAT32("float32", (blockSize, gridPosition, numElements) -> new FloatArrayDataBlock(blockSize, gridPosition, new float[numElements])),
	FLOAT64("float64", (blockSize, gridPosition, numElements) -> new DoubleArrayDataBlock(blockSize, gridPosition, new double[numElements])),
	OBJECT("object", (blockSize, gridPosition, numElements) -> new ByteArrayDataBlock(blockSize, gridPosition, new byte[numElements]));

	private final String label;

	private DataBlockFactory dataBlockFactory;

	private DataType(final String label, final DataBlockFactory dataBlockFactory) {

		this.label = label;
		this.dataBlockFactory = dataBlockFactory;
	}

	@Override
	public String toString() {

		return label;
	}

	public static DataType fromString(final String string) {

		for (final DataType value : values())
			if (value.toString().equals(string))
				return value;
		return null;
	}

	/**
	 * Factory for {@link DataBlock DataBlocks}.
	 *
	 * @param blockSize the block size
	 * @param gridPosition the grid position
	 * @param numElements the number of elements (not necessarily one element per block element)
	 * @return the data block
	 */
	public DataBlock<?> createDataBlock(final int[] blockSize, final long[] gridPosition, final int numElements) {

		return dataBlockFactory.createDataBlock(blockSize, gridPosition, numElements);
	}

	/**
	 * Factory for {@link DataBlock DataBlocks} with one data element for each
	 * block element (e.g. pixel image).
	 *
	 * @param blockSize the block size
	 * @param gridPosition the grid position
	 * @return the data block
	 */
	public DataBlock<?> createDataBlock(final int[] blockSize, final long[] gridPosition) {

		return dataBlockFactory.createDataBlock(blockSize, gridPosition, DataBlock.getNumElements(blockSize));
	}

	private static interface DataBlockFactory {

		public DataBlock<?> createDataBlock(final int[] blockSize, final long[] gridPosition, final int numElements);
	}

	static public class JsonAdapter implements JsonDeserializer<DataType>, JsonSerializer<DataType> {

		@Override
		public DataType deserialize(
				final JsonElement json,
				final Type typeOfT,
				final JsonDeserializationContext context) throws JsonParseException {

			return DataType.fromString(json.getAsString());
		}

		@Override
		public JsonElement serialize(
				final DataType src,
				final Type typeOfSrc,
				final JsonSerializationContext context) {

			return new JsonPrimitive(src.toString());
		}
	}
}