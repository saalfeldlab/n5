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
package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.cache.DelegateStore;
import org.janelia.saalfeldlab.n5.cache.MyJsonCache;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class N5CachedFSTest extends N5FSTest {

	@Override
	protected N5Writer createN5Writer(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		return createN5Writer(location, gson, true);
	}

	@Override
	protected N5Reader createN5Reader(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		return createN5Reader(location, gson, true);
	}

	protected N5Writer createN5Writer(final String location, final GsonBuilder gson, final boolean cache) throws IOException, URISyntaxException {

		return new N5FSWriter(location, gson, cache);
	}

	protected N5Writer createN5Writer(final String location, final boolean cache) throws IOException, URISyntaxException {

		return createN5Writer(location, new GsonBuilder(), cache);
	}

	protected N5Reader createN5Reader(final String location, final GsonBuilder gson, final boolean cache) throws IOException, URISyntaxException {

		return new N5FSReader(location, gson, cache);
	}

	@Override protected N5Writer createN5Writer() throws IOException, URISyntaxException {

		return new N5FSWriter(tempN5Location(), new GsonBuilder(), true) {
			@Override public void close() {

				super.close();
				remove();
			}
		};
	}

	@Test
	public void cacheTest() throws IOException, URISyntaxException {
		/* Test the cache by setting many attributes, then manually deleting the underlying file.
		* The only possible way for the test to succeed is if it never again attempts to read the file, and relies on the cache. */
		try (N5KeyValueWriter n5 = (N5KeyValueWriter) createN5Writer()) {
			final String cachedGroup = "cachedGroup";
			final String attributesPath = n5.getRootedKeyValueAccess().root().resolve(n5.relativeAttributesPath(cachedGroup).uri()).getPath();

			final ArrayList<TestData<?>> tests = new ArrayList<>();
			n5.createGroup(cachedGroup);
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/b/c", 100));
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/a[5]", "asdf"));
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/a[2]", 0));

			Files.delete(Paths.get(attributesPath));
			runTests(n5, tests);
		}

		try (N5KeyValueWriter n5 = (N5KeyValueWriter)createN5Writer(tempN5Location(), false)) {
			final String cachedGroup = "cachedGroup";
			final String attributesPath = n5.getRootedKeyValueAccess().root().resolve(n5.relativeAttributesPath(cachedGroup).uri()).getPath();

			final ArrayList<TestData<?>> tests = new ArrayList<>();
			n5.createGroup(cachedGroup);
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/b/c", 100));
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/a[5]", "asdf"));
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/a[2]", 0));

			Files.delete(Paths.get(attributesPath));
			assertThrows(AssertionError.class, () -> runTests(n5, tests));
			n5.remove();
		}
	}

	@Test
	public void cacheGroupDatasetTest() throws IOException, URISyntaxException {

		final String datasetName = "dd";
		final String groupName = "gg";

		final String tmpLocation = tempN5Location();
		try (GsonKeyValueN5Writer w1 = (GsonKeyValueN5Writer) createN5Writer(tmpLocation);
				GsonKeyValueN5Writer w2 = (GsonKeyValueN5Writer) createN5Writer(tmpLocation);) {

			// create a group, both writers know it exists
			w1.createGroup(groupName);
			assertTrue(w1.exists(groupName));
			assertTrue(w2.exists(groupName));

			// one writer removes the group
			w2.remove(groupName);
			assertTrue(w1.exists(groupName));	// w1's cache thinks group still exists
			assertFalse(w2.exists(groupName));	// w2 knows group has been removed

			// create a dataset
			w1.createDataset(datasetName, dimensions, blockSize, DataType.UINT8, new RawCompression());
			assertTrue(w1.exists(datasetName));
			assertTrue(w2.exists(datasetName));

			assertNotNull(w1.getDatasetAttributes(datasetName));
			assertNotNull(w2.getDatasetAttributes(datasetName));

			// one writer removes the data
			w2.remove(datasetName);
			assertTrue(w1.exists(datasetName));  // w1's cache thinks group still exists
			assertFalse(w2.exists(datasetName)); // w2 knows group has been removed

			assertNotNull(w1.getDatasetAttributes(datasetName));
			assertNull(w2.getDatasetAttributes(datasetName));

			w1.remove();
		}
	}

	@Test
	public void cacheBehaviorTest() throws IOException, URISyntaxException {

		// make an uncached n5 writer
		try (final N5TrackingStorage n5 = new N5TrackingStorage(new RootedFileSystemKeyValueAccess(tempN5Location()),
				new GsonBuilder(), true)) {

			cacheBehaviorHelper(n5);
			n5.remove();
		}
	}

	@Test
	public void cacheListTest() throws IOException, URISyntaxException {

		try (final N5TrackingStorage n5 = new N5TrackingStorage(new RootedFileSystemKeyValueAccess(tempN5Location()),
				new GsonBuilder(), true)) {

			final String groupA = "groupA";
			n5.createGroup(groupA);
			assertEquals(0, n5.getListCallCount());
			assertArrayEquals(new String[] {groupA}, n5.list("/"));
			assertEquals(1, n5.getListCallCount());
			n5.remove(groupA);
			assertArrayEquals(new String[] {}, n5.list("/"));
			assertEquals(1, n5.getListCallCount());

			n5.remove();
		}
	}

	public static void cacheBehaviorHelper(final TrackingStorage n5) throws IOException, URISyntaxException {

		// non existant group
		final String groupA = "groupA";
		final String groupB = "groupB";

		// expected backend method call counts
		int expectedReadAttrCount = 0;
		int expectedWriteAttrCount = 0;
		int expectedIsDirCount = 0;
		int expectedRmDirCount = 0;
		int expectedMkDirCount = 0;
		int expectedListCount = 0;
		n5.resetCounters();

		boolean exists = n5.exists(groupA);
		boolean groupExists = n5.groupExists(groupA);
		boolean datasetExists = n5.datasetExists(groupA);
		assertFalse(exists); // group does not exist
		assertFalse(groupExists); // group does not exist
		assertFalse(datasetExists); // dataset does not exist
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(++expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.createGroup(groupA);
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(++expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// group B
		exists = n5.exists(groupB);
		groupExists = n5.groupExists(groupB);
		datasetExists = n5.datasetExists(groupB);
		assertFalse(exists); // group now exists
		assertFalse(groupExists); // group now exists
		assertFalse(datasetExists); // dataset does not exist
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(++expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		exists = n5.exists(groupA);
		groupExists = n5.groupExists(groupA);
		datasetExists = n5.datasetExists(groupA);
		assertTrue(exists); // group now exists
		assertTrue(groupExists); // group now exists
		assertFalse(datasetExists); // dataset does not exist
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		final String cachedGroup = "cachedGroup";
		// should not check existence when creating a group
		n5.createGroup(cachedGroup);
		n5.createGroup(cachedGroup); // be annoying
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(++expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// should not check existence when this instance created a group
		// should read attributes.json to check whether it is a dataset
		n5.exists(cachedGroup);
		n5.groupExists(cachedGroup);
		n5.datasetExists(cachedGroup);
		assertEquals(++expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// should not read attributes from container when setting them
		n5.setAttribute(cachedGroup, "one", 1);
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(++expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.setAttribute(cachedGroup, "two", 2);
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(++expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.removeAttribute(cachedGroup, "one");
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(++expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// should not write attributes to container when removing a non-existent attribute
		n5.removeAttribute(cachedGroup, "one");
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.removeAttribute(cachedGroup, "cow");
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.removeAttribute(cachedGroup, "two", Integer.class);
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(++expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.list("");
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(++expectedListCount, n5.getListCallCount());

		n5.list(cachedGroup);
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(++expectedListCount, n5.getListCallCount());

		/*
		 * Check existence for groups that have not been made by this reader but isGroup
		 * and isDatatset must be false if it does not exists so then should not be
		 * called.
		 *
		 * Similarly, attributes can not exist for a non-existent group, so should not
		 * attempt to get attributes from the container.
		 *
		 * Finally,listing on a non-existent group is pointless, so don't call the
		 * backend storage
		 */

		final String nonExistentGroup = "doesNotExist";
		n5.exists(nonExistentGroup);
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(++expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.groupExists(nonExistentGroup);
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.datasetExists(nonExistentGroup);
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.getAttributes(nonExistentGroup);
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		assertThrows(N5Exception.class, () -> n5.list(nonExistentGroup));
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		final String a = "a";
		final String ab = "a/b";
		final String abc = "a/b/c";
		// create "a/b/c"
		n5.createGroup(abc);
		assertEquals(++expectedMkDirCount, n5.getMkDirCallCount());

		assertTrue(n5.exists(abc));
		assertTrue(n5.groupExists(abc));
		assertFalse(n5.datasetExists(abc));
		assertEquals(++expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// ensure that backend need not be checked when testing existence of "a/b".
		// ("a/b/attributes.json" needs to be checked to determine whether "a/b" is a dataset.)
		assertTrue(n5.exists(ab));
		assertTrue(n5.groupExists(ab));
		assertFalse(n5.datasetExists(ab));
		assertEquals(++expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// remove a nested group
		// checks for all children should not require a backend check
		n5.remove(a);
		assertEquals(++expectedRmDirCount, n5.getRmDirCallCount());

		assertFalse(n5.exists(a));
		assertFalse(n5.groupExists(a));
		assertFalse(n5.datasetExists(a));
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		assertFalse(n5.exists(ab));
		assertFalse(n5.groupExists(ab));
		assertFalse(n5.datasetExists(ab));
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		assertFalse(n5.exists(abc));
		assertFalse(n5.groupExists(abc));
		assertFalse(n5.datasetExists(abc));
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.createGroup("a");
		assertEquals(++expectedMkDirCount, n5.getMkDirCallCount());
		n5.createGroup("a/a");
		assertEquals(++expectedMkDirCount, n5.getMkDirCallCount());
		n5.createGroup("a/b");
		assertEquals(++expectedMkDirCount, n5.getMkDirCallCount());
		n5.createGroup("a/c");
		assertEquals(++expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		final Set<String> abcListSet = Arrays.stream(n5.list("a")).collect(Collectors.toSet());
		assertEquals(Stream.of("a", "b", "c").collect(Collectors.toSet()), abcListSet);
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(++expectedListCount, n5.getListCallCount());

		// remove a/a
		n5.remove("a/a");
		final Set<String> bc = Arrays.stream(n5.list("a")).collect(Collectors.toSet());
		assertEquals(Stream.of("b", "c").collect(Collectors.toSet()), bc);
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(++expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());


		// Create a new writer and inject a new group with attributes unbeknownst to this writer
		try (N5Writer writer = new N5FSWriter(n5.getURI().toString(), false)) {
			writer.createGroup("sneaky_group");
			writer.setAttribute("sneaky_group", "sneaky_attribute", "BOO!");
		}
		n5.exists("sneaky_group");
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(++expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());
		// every directory is a group, so this shouldn't require any backend calls
		n5.groupExists("sneaky_group");
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());
		// however, to find out whether sneaky_group/ is a dataset, we need to read attributes
		n5.datasetExists("sneaky_group");
		assertEquals(++expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());


		// Create a new writer and inject a new dataset with attributes unbeknownst to this writer
		try (N5Writer writer = new N5FSWriter(n5.getURI().toString(), false)) {
			writer.createDataset("sneaky_dataset", new long[] {10}, new int[] {10}, DataType.UINT8, new RawCompression() );
		}
		n5.datasetExists("sneaky_dataset");
		assertEquals(++expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());
		// we already checked for existence of sneaky_dataset/attributes.json
		// we shouldn't need to check whether directory sneaky_dataset/ exists
		n5.exists("sneaky_dataset");
		assertEquals(expectedReadAttrCount, n5.getReadAttrCallCount());
		assertEquals(expectedWriteAttrCount, n5.getWriteAttrCallCount());
		assertEquals(expectedIsDirCount, n5.getIsDirCallCount());
		assertEquals(expectedRmDirCount, n5.getRmDirCallCount());
		assertEquals(expectedMkDirCount, n5.getMkDirCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());
	}

	public interface TrackingStorage extends CachedGsonKeyValueN5Writer {

		void resetCounters();

		int getReadAttrCallCount();
		int getWriteAttrCallCount();
		int getIsDirCallCount();
		int getRmDirCallCount();
		int getMkDirCallCount();
		int getListCallCount();
	}

	public static class TrackingMetaStore implements DelegateStore {

		int readAttrCallCount = 0;
		int writeAttrCallCount = 0;
		int isDirCallCount = 0;
		int rmDirCallCount = 0;
		int mkDirCallCount = 0;
		int listCallCount = 0;

		private final DelegateStore delegate;

		TrackingMetaStore(final DelegateStore delegate) {
			this.delegate = delegate;
		}

		@Override
		public JsonElement store_readAttributesJson(final N5GroupPath group, final String filename, final Gson gson) throws N5IOException {
			readAttrCallCount++;
			return delegate.store_readAttributesJson(group, filename, gson);
		}

		@Override
		public void store_writeAttributesJson(final N5GroupPath group, final String filename, final JsonElement attributes, final Gson gson) throws N5IOException {
			writeAttrCallCount++;
			delegate.store_writeAttributesJson(group, filename, attributes, gson);
		}

		@Override
		public void store_removeAttributesJson(final N5GroupPath group, final String filename) throws N5IOException {
			delegate.store_removeAttributesJson(group, filename);
		}

		@Override
		public boolean store_isDirectory(final N5GroupPath group) {
			isDirCallCount++;
			return delegate.store_isDirectory(group);
		}

		@Override
		public void store_removeDirectory(final N5GroupPath group) throws N5IOException {
			rmDirCallCount++;
			delegate.store_removeDirectory(group);
		}

		@Override
		public void store_createDirectories(final N5GroupPath group) throws N5IOException {
			mkDirCallCount++;
			delegate.store_createDirectories(group);
		}

		@Override
		public String[] store_listDirectories(final N5GroupPath group) throws N5IOException {
			listCallCount++;
			return delegate.store_listDirectories(group);
		}
	}

	public static class N5TrackingStorage extends N5KeyValueWriter implements TrackingStorage {

		private TrackingMetaStore trackingStore;

		public N5TrackingStorage(final RootedKeyValueAccess keyValueAccess,
				final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws IOException {

			super(keyValueAccess, gsonBuilder, cacheAttributes);
		}

		@Override
		public DelegateStore createMetaStore(
				final RootedKeyValueAccess keyValueAccess,
				final boolean cacheMeta) {

			trackingStore = new TrackingMetaStore(new KeyValueAccessMetaStore(keyValueAccess));
			return cacheMeta ? new MyJsonCache(trackingStore) : trackingStore;
		}

		@Override
		public void resetCounters() {
			trackingStore.readAttrCallCount = 0;
			trackingStore.writeAttrCallCount = 0;
			trackingStore.isDirCallCount = 0;
			trackingStore.rmDirCallCount = 0;
			trackingStore.mkDirCallCount = 0;
			trackingStore.listCallCount = 0;
		}

		@Override
		public int getReadAttrCallCount() {
			return trackingStore.readAttrCallCount;
		}

		@Override
		public int getWriteAttrCallCount() {
			return trackingStore.writeAttrCallCount;
		}

		@Override
		public int getIsDirCallCount() {
			return trackingStore.isDirCallCount;
		}

		@Override
		public int getRmDirCallCount() {
			return trackingStore.rmDirCallCount;
		}

		@Override
		public int getMkDirCallCount() {
			return trackingStore.mkDirCallCount;
		}

		@Override
		public int getListCallCount() {
			return trackingStore.listCallCount;
		}
	}
}
