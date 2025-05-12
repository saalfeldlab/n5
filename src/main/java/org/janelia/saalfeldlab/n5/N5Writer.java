/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
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

import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.shard.Shard;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
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
	 * @param groupPath group path
	 * @param attributePath the key
	 * @param attribute the attribute
	 * @param <T> the attribute type type
	 * @throws N5Exception the exception
	 */
	default <T> void setAttribute(
			final String groupPath,
			final String attributePath,
			final T attribute) throws N5Exception {

		setAttributes(groupPath, Collections.singletonMap(attributePath, attribute));
	}

	/**
	 * Sets a map of attributes.  The passed attributes are inserted into the
	 * existing attribute tree.  New attributes, including their parent
	 * objects will be added, existing attributes whose paths are not included
	 * will remain unchanged, those whose paths are included will be overridden.
	 *
	 * @param groupPath group path
	 * @param attributes the attribute map of attribute paths and values
	 * @throws N5Exception the exception
	 */
	void setAttributes(
			final String groupPath,
			final Map<String, ?> attributes) throws N5Exception;

	/**
	 * Remove the attribute from group {@code pathName} with key {@code key}.
	 *
	 * @param groupPath group path
	 * @param attributePath of attribute to remove
	 * @return true if attribute removed, else false
	 * @throws N5Exception the exception
	 */
	boolean removeAttribute(String groupPath, String attributePath) throws N5Exception;

	/**
	 * Remove the attribute from group {@code pathName} with key {@code key} and
	 * type {@code T}.
	 * <p>
	 * If an attribute at {@code pathName} and {@code key} exists, but is not of
	 * type {@code T}, it is not removed.
	 *
	 * @param groupPath group path
	 * @param attributePath of attribute to remove
	 * @param clazz of the attribute to remove
	 * @param <T> of the attribute
	 * @return the removed attribute, as {@code T}, or {@code null} if no
	 *         matching attribute
	 * @throws N5Exception if removing he attribute failed, parsing the attribute failed, or the attribute cannot be interpreted as T
	 */
	<T> T removeAttribute(String groupPath, String attributePath, Class<T> clazz) throws N5Exception;

	/**
	 * Remove attributes as provided by {@code attributes}.
	 * <p>
	 * If any element of {@code attributes} does not exist, it will be ignored.
	 * If at least one attribute from {@code attributes} is removed, this will
	 * return {@code true}.
	 *
	 *
	 * @param groupPath group path
	 * @param attributePaths to remove
	 * @return true if any of the listed attributes were removed
	 * @throws N5Exception the exception
	 */
	default boolean removeAttributes(final String groupPath, final List<String> attributePaths) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(groupPath);
		boolean removed = false;
		for (final String attribute : attributePaths) {
			removed |= removeAttribute(normalPath, N5URI.normalizeAttributePath(attribute));
		}
		return removed;
	}

	/**
	 * Sets mandatory dataset attributes.
	 *
	 * @param datasetPath dataset path
	 * @param datasetAttributes the dataset attributes
	 * @throws N5Exception the exception
	 */
	default void setDatasetAttributes(
			final String datasetPath,
			final DatasetAttributes datasetAttributes) throws N5Exception {

		setAttribute(datasetPath, "/", datasetAttributes);
	}

	/**
	 * Set the SemVer version of this container as specified in the
	 * {@link N5Reader#VERSION_KEY} attribute of the root group. This default
	 * implementation writes the version only if the current version is not
	 * equal {@link N5Reader#VERSION}.
	 *
	 * @throws N5Exception the exception
	 */
	default void setVersion() throws N5Exception {

		if (!VERSION.equals(getVersion()))
			setAttribute("/", VERSION_KEY, VERSION.toString());
	}

	/**
	 * Creates a group (directory)
	 *
	 * @param groupPath the path
	 * @throws N5Exception the exception
	 */
	void createGroup(final String groupPath) throws N5Exception;

	/**
	 * Removes a group or dataset (directory and all contained files).
	 *
	 * <p>
	 * <code>{@link #remove(String) remove("")}</code> or
	 * <code>{@link #remove(String) remove("")}</code> will delete this N5
	 * container. Please note that no checks for safety will be performed,
	 * e.g. <code>{@link #remove(String) remove("..")}</code> will try to
	 * recursively delete the parent directory of this N5 container which
	 * only fails because it attempts to delete the parent directory before it
	 * is empty.
	 *
	 * @param groupPath group path
	 * @return true if removal was successful, false otherwise
	 * @throws N5Exception the exception
	 */
	boolean remove(final String groupPath) throws N5Exception;

	/**
	 * Removes the N5 container.
	 *
	 * @return true if removal was successful, false otherwise
	 * @throws N5Exception the exception
	 */
	default boolean remove() throws N5Exception {

		return remove("/");
	}

	/**
	 * Creates a dataset. This does not create any data but the path and
	 * mandatory attributes only.
	 *
	 * @param datasetPath dataset path
	 * @param datasetAttributes the dataset attributes
	 * @throws N5Exception the exception
	 */
	default void createDataset(
			final String datasetPath,
			final DatasetAttributes datasetAttributes) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(datasetPath);
		createGroup(normalPath);
		setDatasetAttributes(normalPath, datasetAttributes);
	}

	/**
	 * Creates a dataset. This does not create any data but the path and
	 * mandatory attributes only.
	 *
	 * @param datasetPath dataset path
	 * @param dimensions the dataset dimensions
	 * @param blockSize the block size
	 * @param dataType the data type
	 * @param codecs codecs to encode/decode with
	 * @throws N5Exception the exception
	 */
	default void createDataset(
			final String datasetPath,
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Codec... codecs) throws N5Exception {

		createDataset(datasetPath, new DatasetAttributes(dimensions, blockSize, dataType, codecs));
	}

	/**
	 * DEPRECATED. {@link Compression}s are {@link Codec}s.
	 * Use {@link #createDataset(String, long[], int[], DataType, Codec...)}
	 * <p> </p>
	 * Creates a dataset. This does not create any data but the path and
	 * mandatory attributes only.
	 *
	 * @param datasetPath dataset path
	 * @param dimensions the dataset dimensions
	 * @param blockSize the block size
	 * @param dataType the data type
	 * @param compression the compression
	 * @throws N5Exception the exception
	 */
	@Deprecated
	default void createDataset(
			final String datasetPath,
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Compression compression) throws N5Exception {

		createDataset(datasetPath, dimensions, blockSize, dataType, new N5BlockCodec<>(), compression);
	}

	/**
	 * Writes a {@link DataBlock}.
	 *
	 * @param datasetPath dataset path
	 * @param datasetAttributes the dataset attributes
	 * @param dataBlock the data block
	 * @param <T> the data block data type
	 * @throws N5Exception the exception
	 */
	<T> void writeBlock(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws N5Exception;

	/**
	 * Write multiple data blocks, useful for request aggregation .
	 *
	 * @param datasetPath dataset path
	 * @param datasetAttributes the dataset attributes
	 * @param dataBlocks the data block
	 * @param <T> the data block data type
	 * @throws N5Exception the exception
	 */
	default <T> void writeBlocks(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T>... dataBlocks) throws N5Exception {

		// default method is naive
		for (DataBlock<T> block : dataBlocks)
			writeBlock(datasetPath, datasetAttributes, block);
	}

	/**
	 * Writes a {@link Shard}.
	 *
	 * @param datasetPath dataset path
	 * @param datasetAttributes the dataset attributes
	 * @param shard the shard
	 * @param <T> the data block data type
	 * @throws N5Exception the exception
	 */
	<T> void writeShard(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final Shard<T> shard) throws N5Exception;

	/**
	 * Deletes the block at {@code gridPosition}
	 *
	 * @param datasetPath dataset path
	 * @param gridPosition position of block to be deleted
	 * @throws N5Exception the exception
	 *
	 * @return {@code true} if the block at {@code gridPosition} is "empty"
	 *         after
	 *         deletion. The meaning of "empty" is implementation dependent. For
	 *         example "empty" means that no file exists on the file system for
	 *         the
	 *         deleted block in case of the file system implementation.
	 *
	 */
	boolean deleteBlock(
			final String datasetPath,
			final long... gridPosition) throws N5Exception;

	/**
	 * Save a {@link Serializable} as an N5 {@link DataBlock} at a given offset.
	 * The
	 * offset is given in {@link DataBlock} grid coordinates.
	 *
	 * @param object the object to serialize
	 * @param datasetPath the dataset path
	 * @param datasetAttributes the dataset attributes
	 * @param gridPosition the grid position
	 * @throws N5Exception the exception
	 */
	default void writeSerializedBlock(
			final Serializable object,
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		try (ObjectOutputStream out = new ObjectOutputStream(byteOutputStream)) {
			out.writeObject(object);
		} catch (final IOException | UncheckedIOException e) {
			throw new N5Exception.N5IOException(e);
		}
		final byte[] bytes = byteOutputStream.toByteArray();
		final DataBlock<?> dataBlock = new ByteArrayDataBlock(null, gridPosition, bytes);
		writeBlock(datasetPath, datasetAttributes, dataBlock);
	}
}
