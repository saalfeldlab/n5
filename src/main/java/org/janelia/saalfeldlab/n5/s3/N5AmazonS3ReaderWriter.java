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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.janelia.saalfeldlab.n5.AbstractN5ReaderWriter;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

/**
 * Amazon Web Services S3-based N5 implementation.
 *
 * @author Igor Pisarev
 */
public class N5AmazonS3ReaderWriter extends AbstractN5ReaderWriter {

	private static final String jsonFile = "attributes.json";
	private static final String delimiter = "/";

	private final AmazonS3 s3;
	private final String bucketName;
	private final Gson gson;

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
	public N5AmazonS3ReaderWriter(final AmazonS3 s3, final String bucketName, final GsonBuilder gsonBuilder) {

		this.s3 = s3;
		this.bucketName = bucketName;

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
	public N5AmazonS3ReaderWriter(final AmazonS3 s3, final String bucketName) {

		this(s3, bucketName, new GsonBuilder());
	}

	@Override
	public void createContainer() throws IOException {

		if (!s3.doesBucketExistV2(bucketName))
			s3.createBucket(bucketName);
		createGroup("");
	}

	@Override
	public void removeContainer() throws IOException {

		remove("");
		s3.deleteBucket(bucketName);
	}

	@Override
	public boolean exists(final String pathName) {

		final String metadataKey = Paths.get(pathName, jsonFile).toString();
		return s3.doesObjectExist(bucketName, removeFrontDelimiter(metadataKey));
	}

	@Override
	public void createGroup(final String pathName) throws IOException {

		final Path path = Paths.get(pathName);
		for (int i = 0; i < path.getNameCount(); ++i) {
			final String subgroup = path.subpath(0, i + 1).toString();
			if (!exists(subgroup))
				setAttributes(subgroup, Collections.emptyMap());
		}
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
		if (!s3.doesObjectExist(bucketName, removeFrontDelimiter(metadataKey)))
			return new HashMap<>();

		try (final InputStream in = s3.getObject(bucketName, removeFrontDelimiter(metadataKey)).getObjectContent()) {
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
	public void setAttributes(final String pathName, final Map<String, ?> attributes) throws IOException {

		final String metadataKey = Paths.get(pathName, jsonFile).toString();
		final HashMap<String, JsonElement> map = getAttributes(pathName);
		for (final Entry<String, ?> entry : attributes.entrySet())
			map.put(entry.getKey(), gson.toJsonTree(entry.getValue()));

		final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
 		final String json = gson.toJson(map, mapType);

		final byte[] bytes = json.getBytes("UTF-8");
		try (final InputStream data = new ByteArrayInputStream(bytes)) {
			final ObjectMetadata objectMetadata = new ObjectMetadata();
			objectMetadata.setContentLength(bytes.length);
			s3.putObject(bucketName, removeFrontDelimiter(metadataKey), data, objectMetadata);
		}
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException {

		final String dataBlockKey = getDataBlockPath(pathName, gridPosition).toString();
		if (!s3.doesObjectExist(bucketName, removeFrontDelimiter(dataBlockKey)))
			return null;

		try (final InputStream in = s3.getObject(bucketName, removeFrontDelimiter(dataBlockKey)).getObjectContent()) {
			return readBlock(Channels.newChannel(in), datasetAttributes, gridPosition);
		}
	}

	@Override
	public <T> void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		final String dataBlockKey = getDataBlockPath(pathName, dataBlock.getGridPosition()).toString();
		try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			writeBlock(Channels.newChannel(byteStream), datasetAttributes, dataBlock);
			final byte[] bytes = byteStream.toByteArray();
			try (final InputStream data = new ByteArrayInputStream(bytes)) {
				final ObjectMetadata objectMetadata = new ObjectMetadata();
				objectMetadata.setContentLength(bytes.length);
				s3.putObject(bucketName, removeFrontDelimiter(dataBlockKey), data, objectMetadata);
			}
		}
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		final String correctedPathName = removeFrontDelimiter(pathName);
		final String prefix = !correctedPathName.isEmpty() ? appendDelimiter(correctedPathName) : correctedPathName;
		final Path path = Paths.get(prefix);

		final List<String> subGroups = new ArrayList<>();
		final ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
				.withBucketName(bucketName)
				.withPrefix(prefix)
				.withDelimiter(delimiter);
		ListObjectsV2Result objectsListing;
		do {
			objectsListing = s3.listObjectsV2(listObjectsRequest);
			for (final String commonPrefix : objectsListing.getCommonPrefixes()) {
				if (exists(commonPrefix)) {
					final Path relativePath = path.relativize(Paths.get(commonPrefix));
					subGroups.add(relativePath.toString());
				}
			}
			listObjectsRequest.setContinuationToken(objectsListing.getNextContinuationToken());
		} while (objectsListing.isTruncated());
		return subGroups.toArray(new String[subGroups.size()]);
	}

	/**
	 * Removes a group or dataset (directory and all contained files).
	 *
	 * <p><code>{@link #remove(String) remove("")}</code> or
	 * <code>{@link #remove(String) remove("")}</code> will delete this N5
	 * container.  Please note that no checks for safety will be performed,
	 * e.g. <code>{@link #remove(String) remove("..")}</code> will try to
	 * recursively delete the parent directory of this N5 container which
	 * only fails because it attempts to delete the parent directory before it
	 * is empty.
	 *
	 * @param pathName group path
	 * @throws IOException
	 */
	@Override
	public boolean remove(final String pathName) throws IOException {

		final String correctedPathName = removeFrontDelimiter(pathName);
		final String prefix = !correctedPathName.isEmpty() ? appendDelimiter(correctedPathName) : correctedPathName;
		final ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
				.withBucketName(bucketName)
				.withPrefix(prefix);
		ListObjectsV2Result objectsListing;
		do {
			objectsListing = s3.listObjectsV2(listObjectsRequest);
			final List<String> objectsToDelete = new ArrayList<>();
			for (final S3ObjectSummary object : objectsListing.getObjectSummaries())
				objectsToDelete.add(object.getKey());

			if (!objectsToDelete.isEmpty()) {
				s3.deleteObjects(new DeleteObjectsRequest(bucketName)
						.withKeys(objectsToDelete.toArray(new String[objectsToDelete.size()]))
					);
			}
			listObjectsRequest.setContinuationToken(objectsListing.getNextContinuationToken());
		} while (objectsListing.isTruncated());
		return !exists(pathName);
	}

	/**
	 * When absolute paths are passed (e.g. /group/data), AWS S3 service creates an additional root folder with an empty name.
	 * This method removes the root slash symbol and returns the corrected path.
	 *
	 * @param pathName
	 * @return
	 */
	private static String removeFrontDelimiter(final String pathName) {

		return pathName.startsWith(delimiter) ? pathName.substring(1) : pathName;
	}

	/**
	 * When listing children objects for a group, must append a delimiter to the path (e.g. group/data/).
	 * This is necessary for not including wrong objects in the filtered set (e.g. group/data-2/attributes.json when group/data is passed without the last slash)
	 *
	 * @param pathName
	 * @return
	 */
	private static String appendDelimiter(final String pathName) {

		return pathName.endsWith(delimiter) ? pathName : pathName + delimiter;
	}
}
