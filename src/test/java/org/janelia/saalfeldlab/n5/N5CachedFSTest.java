package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.junit.Test;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

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

	@Test
	public void cacheTest() throws IOException, URISyntaxException {
		/* Test the cache by setting many attributes, then manually deleting the underlying file.
		* The only possible way for the test to succeed is if it never again attempts to read the file, and relies on the cache. */
		try (N5KeyValueWriter n5 = (N5KeyValueWriter) createN5Writer()) {
			final String cachedGroup = "cachedGroup";
			final String attributesPath = n5.absoluteAttributesPath(cachedGroup);


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
			final String attributesPath = n5.absoluteAttributesPath(cachedGroup);

			final ArrayList<TestData<?>> tests = new ArrayList<>();
			n5.createGroup(cachedGroup);
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/b/c", 100));
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/a[5]", "asdf"));
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/a[2]", 0));

			Files.delete(Paths.get(attributesPath));
			assertThrows(AssertionError.class, () -> runTests(n5, tests));
		}
	}

	@Test
	public void cacheGroupDatasetTest() throws IOException, URISyntaxException {

		final String datasetName = "dd";
		final String groupName = "gg";

		final String tmpLocation = tempN5Location();
		try (N5KeyValueWriter w1 = (N5KeyValueWriter) createN5Writer(tmpLocation);
				N5KeyValueWriter w2 = (N5KeyValueWriter) createN5Writer(tmpLocation);) {

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
		}
	}

	@Test
	public void cacheBehaviorTest() throws IOException, URISyntaxException {

		final String loc = tempN5Location();
		// make an uncached n5 writer
		try (final N5TrackingStorage n5 = new N5TrackingStorage(new FileSystemKeyValueAccess(FileSystems.getDefault()), loc,
				new GsonBuilder(), true)) {

			cacheBehaviorHelper(n5);
		}
	}

	public static void cacheBehaviorHelper(final TrackingStorage n5) throws IOException, URISyntaxException {

		// non existant group
		final String groupA = "groupA";
		final String groupB = "groupB";

		// expected backend method call counts
		int expectedExistCount = 0;
		final int expectedGroupCount = 0;
		final int expectedDatasetCount = 0;
		final int expectedAttributeCount = 0;
		int expectedListCount = 0;

		boolean exists = n5.exists(groupA);
		boolean groupExists = n5.groupExists(groupA);
		boolean datasetExists = n5.datasetExists(groupA);
		assertFalse(exists); // group does not exist
		assertFalse(groupExists); // group does not exist
		assertFalse(datasetExists); // dataset does not exist
		assertEquals(++expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());

		n5.createGroup(groupA);
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());

		// group B
		exists = n5.exists(groupB);
		groupExists = n5.groupExists(groupB);
		datasetExists = n5.datasetExists(groupB);
		assertFalse(exists); // group now exists
		assertFalse(groupExists); // group now exists
		assertFalse(datasetExists); // dataset does not exist
		assertEquals(++expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());

		exists = n5.exists(groupA);
		groupExists = n5.groupExists(groupA);
		datasetExists = n5.datasetExists(groupA);
		assertTrue(exists); // group now exists
		assertTrue(groupExists); // group now exists
		assertFalse(datasetExists); // dataset does not exist
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());

		final String cachedGroup = "cachedGroup";
		// should not check existence when creating a group
		n5.createGroup(cachedGroup);
		n5.createGroup(cachedGroup); // be annoying
		assertEquals(++expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// should not check existence when this instance created a group
		n5.exists(cachedGroup);
		n5.groupExists(cachedGroup);
		n5.datasetExists(cachedGroup);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// should not read attributes from container when setting them
		System.out.println(n5.getAttrCallCount());
		n5.setAttribute(cachedGroup, "one", 1);
		System.out.println(n5.getAttrCallCount());
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.setAttribute(cachedGroup, "two", 2);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.list("");
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(++expectedListCount, n5.getListCallCount());

		n5.list(cachedGroup);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
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
		assertEquals(++expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.groupExists(nonExistentGroup);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.datasetExists(nonExistentGroup);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.getAttributes(nonExistentGroup);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.list(nonExistentGroup);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		final String a = "a";
		final String ab = "a/b";
		final String abc = "a/b/c";
		// create "a/b/c"
		n5.createGroup(abc);
		assertTrue(n5.exists(abc));
		assertTrue(n5.groupExists(abc));
		assertFalse(n5.datasetExists(abc));
		assertEquals(++expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// ensure that backend need not be checked when testing existence of "a/b"
		// TODO how does this work
		assertTrue(n5.exists(ab));
		assertTrue(n5.groupExists(ab));
		assertFalse(n5.datasetExists(ab));
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// remove a nested group
		// checks for all children should not require a backend check
		n5.remove(a);
		assertFalse(n5.exists(a));
		assertFalse(n5.groupExists(a));
		assertFalse(n5.datasetExists(a));
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		assertFalse(n5.exists(ab));
		assertFalse(n5.groupExists(ab));
		assertFalse(n5.datasetExists(ab));
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		assertFalse(n5.exists(abc));
		assertFalse(n5.groupExists(abc));
		assertFalse(n5.datasetExists(abc));
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.createGroup("a");
		assertEquals(expectedExistCount, n5.getExistCallCount());
		n5.createGroup("a/a");
		assertEquals(++expectedExistCount, n5.getExistCallCount());
		n5.createGroup("a/b");
		assertEquals(expectedExistCount, n5.getExistCallCount());
		n5.createGroup("a/c");
		assertEquals(++expectedExistCount, n5.getExistCallCount());

		assertArrayEquals(new String[] {"a", "b", "c"}, n5.list("a")); // call list
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(++expectedListCount, n5.getListCallCount()); // list incremented

		// remove a
		n5.remove("a/a");
		assertArrayEquals(new String[] {"b", "c"}, n5.list("a")); // call list
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount()); // list NOT incremented

		// TODO repeat the above exercise when creating dataset
	}

	public static interface TrackingStorage extends CachedGsonKeyValueN5Writer {

		public int getAttrCallCount();
		public int getExistCallCount();
		public int getGroupCallCount();
		public int getGroupAttrCallCount();
		public int getDatasetCallCount();
		public int getDatasetAttrCallCount();
		public int getListCallCount();
	}

	public static class N5TrackingStorage extends N5KeyValueWriter implements TrackingStorage {

		public int attrCallCount = 0;
		public int existsCallCount = 0;
		public int groupCallCount = 0;
		public int groupAttrCallCount = 0;
		public int datasetCallCount = 0;
		public int datasetAttrCallCount = 0;
		public int listCallCount = 0;

		public N5TrackingStorage(final KeyValueAccess keyValueAccess, final String basePath,
				final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws IOException {

			super(keyValueAccess, basePath, gsonBuilder, cacheAttributes);
		}

		@Override
		public JsonElement getAttributesFromContainer(final String key, final String cacheKey) {
			attrCallCount++;
			return super.getAttributesFromContainer(key, cacheKey);
		}

		@Override
		public boolean existsFromContainer(final String path, final String cacheKey) {
			existsCallCount++;
			return super.existsFromContainer(path, cacheKey);
		}

		@Override
		public boolean isGroupFromContainer(final String key) {
			groupCallCount++;
			return super.isGroupFromContainer(key);
		}

		@Override
		public boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes) {
			groupAttrCallCount++;
			return super.isGroupFromAttributes(normalCacheKey, attributes);
		}

		@Override
		public boolean isDatasetFromContainer(final String key) {
			datasetCallCount++;
			return super.isDatasetFromContainer(key);
		}

		@Override
		public boolean isDatasetFromAttributes(final String normalCacheKey, final JsonElement attributes) {
			datasetAttrCallCount++;
			return super.isDatasetFromAttributes(normalCacheKey, attributes);
		}

		@Override
		public String[] listFromContainer(final String key) {
			listCallCount++;
			return super.listFromContainer(key);
		}

		@Override
		public int getAttrCallCount() {
			return attrCallCount;
		}

		@Override
		public int getExistCallCount() {
			return existsCallCount;
		}

		@Override
		public int getGroupCallCount() {
			return groupCallCount;
		}

		@Override
		public int getGroupAttrCallCount() {
			return groupAttrCallCount;
		}

		@Override
		public int getDatasetCallCount() {
			return datasetCallCount;
		}

		@Override
		public int getDatasetAttrCallCount() {
			return datasetAttrCallCount;
		}

		@Override
		public int getListCallCount() {
			return listCallCount;
		}
	}

}
