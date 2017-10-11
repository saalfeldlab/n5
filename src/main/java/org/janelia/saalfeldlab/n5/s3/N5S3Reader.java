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
package org.janelia.saalfeldlab.n5.s3;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.janelia.saalfeldlab.n5.BlockReader;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

/**
 * Amazon Web Services S3 implementation of the {@link N5Reader} interface.
 *
 * @author Igor Pisarev
 */
public class N5S3Reader implements N5Reader {

	protected static final String jsonFile = "attributes.json";

	protected final AmazonS3 s3;
	protected final String bucket;
	protected final Gson gson;

	/**
	 * Opens an {@link N5Reader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * If the base path does not exist, it will not be created and all
	 * subsequent attempts to read attributes, groups, or datasets
	 * will fail with an {@link IOException}.
	 *
	 * @param basePath n5 base path
	 * @param gsonBuilder
	 */
	public N5S3Reader(final AmazonS3 s3, final String bucket, final GsonBuilder gsonBuilder) {

		this.s3 = s3;
		this.bucket = bucket;

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeAdapter(CompressionType.class, new CompressionType.JsonAdapter());
		this.gson = gsonBuilder.create();
	}

	/**
	 * Opens an {@link N5Reader} at a given base path.
	 *
	 * If the base path does not exist, it will not be created and all
	 * subsequent attempts to read or write attributes, groups, or datasets
	 * will fail with an {@link IOException}.
	 *
	 * @param basePath n5 base path
	 */
	public N5S3Reader(final AmazonS3 s3, final String bucket) {

		this(s3, bucket, new GsonBuilder());
	}

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName group path
	 * @return
	 * @throws IOException
	 *
	 * TODO uses file locks to synchronize with other processes, now also
	 *   synchronize for threads inside the JVM
	 */
	@Override
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException {

		final String metadataKey = Paths.get(pathName, jsonFile).toString();
		try (final InputStream in = s3.getObject(bucket, metadataKey).getObjectContent()) {
			final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
			final HashMap<String, JsonElement> map = gson.fromJson(new InputStreamReader(in, "UTF-8"), mapType);
			return map == null ? new HashMap<>() : map;
		}
	}

	@Override
	public <T> T getAttribute(final String pathName, final String key, final Class<T> clazz) throws IOException {
		final HashMap<String, JsonElement> map = getAttributes(pathName);
		final JsonElement attribute = map.get(key);
		if (attribute != null)
			return gson.fromJson(attribute, clazz);
		else
			return null;
	}

	@Override
	public DatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		final HashMap<String, JsonElement> attributes = getAttributes(pathName);

		final JsonElement dimensionsElement = attributes.get(DatasetAttributes.dimensionsKey);
		if (dimensionsElement == null)
			return null;
		final long[] dimensions = gson.fromJson(dimensionsElement, long[].class);
		if (dimensions == null)
			return null;

		final JsonElement dataTypeElement = attributes.get(DatasetAttributes.dataTypeKey);
		if (dataTypeElement == null)
			return null;
		final DataType dataType = gson.fromJson(dataTypeElement, DataType.class);
		if (dataType == null)
			return null;

		final JsonElement blockSizeElement = attributes.get(DatasetAttributes.blockSizeKey);
		int[] blockSize = null;
		if (blockSizeElement != null)
			blockSize = gson.fromJson(blockSizeElement, int[].class);
		if (blockSize == null)
			blockSize = Arrays.stream(dimensions).mapToInt(a -> (int)a).toArray();

		final JsonElement compressionTypeElement = attributes.get(DatasetAttributes.compressionTypeKey);
		CompressionType compressionType = null;
		if (compressionTypeElement == null)
			return null;
		compressionType = gson.fromJson(compressionTypeElement, CompressionType.class);
		if (compressionType == null)
			compressionType = CompressionType.RAW;

		return new DatasetAttributes(dimensions, blockSize, dataType, compressionType);
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException {

		final String dataBlockRelativePath = getDataBlockPath(pathName, gridPosition).toString();
		final String dataBlockKey = Paths.get(pathName, dataBlockRelativePath).toString();
		if (!s3.doesObjectExist(bucket, dataBlockKey))
			return null;

		try (final InputStream in = s3.getObject(bucket, dataBlockKey).getObjectContent()) {
			final DataInputStream dis = new DataInputStream(in);
			final short mode = dis.readShort();
			final int nDim = dis.readShort();
			final int[] blockSize = new int[nDim];
			for (int d = 0; d < nDim; ++d)
				blockSize[d] = dis.readInt();
			final int numElements;
			switch (mode) {
			case 1:
				numElements = dis.readInt();
				break;
			default:
				numElements = DataBlock.getNumElements(blockSize);
			}
			final DataBlock<?> dataBlock = datasetAttributes.getDataType().createDataBlock(blockSize, gridPosition, numElements);

			final BlockReader reader = datasetAttributes.getCompressionType().getReader();
			reader.read(dataBlock, Channels.newChannel(in));
			return dataBlock;
		}
	}

	@Override
	public boolean exists(final String pathName) {

		final String metadataKey = Paths.get(pathName, jsonFile).toString();
		return s3.doesObjectExist(bucket, metadataKey);
	}

	@Override
	public boolean datasetExists(final String pathName) throws IOException {

		return exists(pathName) && getDatasetAttributes(pathName) != null;
	}

	/**
	 * Creates the path for a data block in a dataset at a given grid position.
	 *
	 * The returned path is
	 * <pre>
	 * $datasetPathName/$gridPosition[0]/$gridPosition[1]/.../$gridPosition[n]
	 * </pre>
	 *
	 * This is the file into which the data block will be stored.
	 *
	 * @param datasetPathName
	 * @param gridPosition
	 * @return
	 */
	public static Path getDataBlockPath(final String datasetPathName, final long[] gridPosition) {

		final String[] pathComponents = new String[gridPosition.length];
		for (int i = 0; i < pathComponents.length; ++i)
			pathComponents[i] = Long.toString(gridPosition[i]);

		return Paths.get(
				datasetPathName,
				pathComponents);
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		final List<String> subGroups = new ArrayList<>();
		ObjectListing objectsListing = null;
		do {
			if (objectsListing == null) {
				objectsListing = s3.listObjects(new ListObjectsRequest()
						.withBucketName(bucket)
						.withPrefix(pathName)
						.withDelimiter("/")
					);
			} else {
				objectsListing = s3.listNextBatchOfObjects(objectsListing);
			}
			for (final String commonPrefix : objectsListing.getCommonPrefixes())
				if (exists(commonPrefix))
					subGroups.add(commonPrefix);
		} while (objectsListing.isTruncated());
		return subGroups.toArray(new String[subGroups.size()]);
	}
}
