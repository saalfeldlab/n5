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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A simple structured container API for hierarchies of chunked
 * n-dimensional datasets and attributes.
 *
 * @author Stephan Saalfeld
 * @see "https://github.com/axtimwalde/n5"
 */
public interface N5Writer extends N5Reader {

	/**
	 * Sets an attribute.
	 *
	 * @param pathName group path
	 * @param key the key
	 * @param attribute the attribute
	 * @param <T> the attribute type type
	 * @throws IOException the exception
	 */
	default <T> void setAttribute(
			final String pathName,
			final String key,
			final T attribute) throws IOException {

		setAttributes(pathName, Collections.singletonMap(key, attribute));
	}

	/**
	 * Sets a map of attributes.
	 *
	 * @param pathName group path
	 * @param attributes the attribute map
	 * @throws IOException the exception
	 */
	void setAttributes(
			final String pathName,
			final Map<String, ?> attributes) throws IOException;

	/**
	 * Remove the attribute from group {@code pathName} with key {@code key}.
	 *
	 * @param pathName group path
	 * @param key of attribute to remove
	 * @return true if attribute removed, else false
	 * @throws IOException the exception
	 */
	boolean removeAttribute(String pathName, String key) throws IOException;

	/**
	 * Remove the attribute from group {@code pathName} with key {@code key} and type {@code T}.
	 * <p>
	 * If an attribute at {@code pathName} and {@code key} exists, but is not of type {@code T}, it is not removed.
	 *
	 * @param pathName group path
	 * @param key of attribute to remove
	 * @param clazz of the attribute to remove
	 * @param <T> of the attribute
	 * @return the removed attribute, as {@code T}, or {@code null} if no matching attribute
	 * @throws IOException the exception
	 */
	<T> T removeAttribute(String pathName, String key, Class<T> clazz) throws IOException;

	/**
	 * Remove attributes as provided by {@code attributes}.
	 * <p>
	 * If any element of {@code attributes} does not exist, it will be ignored.
	 * If at least one attribute from {@code attributes} is removed, this will return {@code true}.
	 *
	 *
	 * @param pathName group path
	 * @param attributes to remove
	 * @return true if any of the listed attributes were removed
	 * @throws IOException the exception
	 */
	boolean removeAttributes( String pathName, List<String> attributes) throws IOException;

	/**
	 * Sets mandatory dataset attributes.
	 *
	 * @param pathName dataset path
	 * @param datasetAttributes the dataset attributse
	 * @throws IOException the exception
	 */
	default void setDatasetAttributes(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws IOException {

		setAttributes(pathName, datasetAttributes.asMap());
	}

	/**
	 * Creates a group (directory)
	 *
	 * @param pathName the path
	 * @throws IOException the exception
	 */
	void createGroup(final String pathName) throws IOException;

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
	 * @return true if removal was successful, false otherwise
	 * @throws IOException the exception
	 */
	boolean remove(final String pathName) throws IOException;

	/**
	 * Removes the N5 container.
	 *
	 * @return true if removal was successful, false otherwise
	 * @throws IOException the exception
	 */
	default boolean remove() throws IOException {

		return remove("/");
	}

	/**
	 * Creates a dataset.  This does not create any data but the path and
	 * mandatory attributes only.
	 *
	 * @param pathName dataset path
	 * @param datasetAttributes the dataset attributes
	 * @throws IOException the exception
	 */
	default void createDataset(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws IOException {

		createGroup(pathName);
		setDatasetAttributes(pathName, datasetAttributes);
	}

	/**
	 * Creates a dataset. This does not create any data but the path and mandatory
	 * attributes only.
	 *
	 * @param pathName    dataset path
	 * @param dimensions  the dataset dimensions
	 * @param blockSize   the block size
	 * @param dataType    the data type
	 * @param compression the compression
	 * @throws IOException the exception
	 */
	default void createDataset(
			final String pathName,
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Compression compression) throws IOException {

		createDataset(pathName, new DatasetAttributes(dimensions, blockSize, dataType, compression));
	}

	/**
	 * Writes a {@link DataBlock}.
	 *
	 * @param pathName          dataset path
	 * @param datasetAttributes the dataset attributes
	 * @param dataBlock         the data block
	 * @param <T>               the data block data type
	 * @throws IOException the exception
	 */
	<T> void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException;


	/**
	 * Deletes the block at {@code gridPosition}
	 *
	 * @param pathName     dataset path
	 * @param gridPosition position of block to be deleted
	 * @throws IOException the exception
	 *
	 * @return {@code true} if the block at {@code gridPosition} is "empty" after
	 *         deletion. The meaning of "empty" is implementation dependent. For
	 *         example "empty" means that no file exists on the file system for the
	 *         deleted block in case of the file system implementation.
	 *
	 */
	boolean deleteBlock(
			final String pathName,
			final long... gridPosition) throws IOException;

	/**
	 * Save a {@link Serializable} as an N5 {@link DataBlock} at a given offset. The
	 * offset is given in {@link DataBlock} grid coordinates.
	 *
	 * @param object            the object to serialize
	 * @param dataset           the dataset path
	 * @param datasetAttributes the dataset attributes
	 * @param gridPosition      the grid position
	 * @throws IOException the exception
	 */
	default void writeSerializedBlock(
			final Serializable object,
			final String dataset,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws IOException {

		final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		try (ObjectOutputStream out = new ObjectOutputStream(byteOutputStream)) {
			out.writeObject(object);
		}
		final byte[] bytes = byteOutputStream.toByteArray();
		final DataBlock<?> dataBlock = new ByteArrayDataBlock(null, gridPosition, bytes);
		writeBlock(dataset, datasetAttributes, dataBlock);
	}
}
