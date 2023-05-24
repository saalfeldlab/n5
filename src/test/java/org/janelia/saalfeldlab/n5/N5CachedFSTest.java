package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

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

		final String basePath = new File(new URI(location)).getCanonicalPath();
		return new N5FSWriter(basePath, gson, cache);
	}

	protected N5Writer createN5Writer(final String location, final boolean cache) throws IOException, URISyntaxException {

		return createN5Writer(location, new GsonBuilder(), cache);
	}

	protected N5Reader createN5Reader(final String location, final GsonBuilder gson, final boolean cache) throws IOException, URISyntaxException {

		final String basePath = new File(new URI(location)).getCanonicalPath();
		return new N5FSReader(basePath, gson, cache);
	}

	@Test
	public void cacheTest() throws IOException, URISyntaxException {
		/* Test the cache by setting many attributes, then manually deleting the underlying file.
		* The only possible way for the test to succeed is if it never again attempts to read the file, and relies on the cache. */

		try (N5KeyValueWriter n5 = (N5KeyValueWriter) createN5Writer()) {
			final String cachedGroup = "cachedGroup";
			final String attributesPath = n5.attributesPath(cachedGroup);


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
			final String attributesPath = n5.attributesPath(cachedGroup);

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
	public void emptyCacheInfoBehavior() throws IOException, URISyntaxException {

		final String loc = tempN5Location();
		// make an uncached n5 writer
		try (final TrackingStorage n5 = new TrackingStorage(new FileSystemKeyValueAccess(FileSystems.getDefault()), loc,
				new GsonBuilder(), true)) {

			final String group = "mygroup";
			boolean groupExists = n5.exists(group);
			assertFalse(groupExists); // group does not exist
			assertEquals(1, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);

			n5.createGroup(group);
			groupExists = n5.exists(group);
			assertTrue(groupExists); // group now exists
			assertEquals(1, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
		}
	}

	@Test
	public void attributeListingSeparationTest() throws IOException, URISyntaxException {

		final String loc = tempN5Location();
		// make an uncached n5 writer
		try (final TrackingStorage n5 = new TrackingStorage(
				new FileSystemKeyValueAccess(FileSystems.getDefault()), loc, new GsonBuilder(), true)) {

			final String cachedGroup = "cachedGroup";
			// should not check existence when creating a group
			n5.createGroup(cachedGroup);
			n5.createGroup(cachedGroup); // be annoying
			assertEquals(0, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
			assertEquals(0, n5.listCallCount);

			// should not check existence when this instance created a group
			n5.exists(cachedGroup);
			n5.groupExists(cachedGroup);
			n5.datasetExists(cachedGroup);
			assertEquals(0, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
			assertEquals(0, n5.listCallCount);

			// should not read attributes from container when setting them
			n5.setAttribute(cachedGroup, "one", 1);
			assertEquals(0, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
			assertEquals(0, n5.listCallCount);

			n5.setAttribute(cachedGroup, "two", 2);
			assertEquals(0, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
			assertEquals(0, n5.listCallCount);

			n5.list("/");
			assertEquals(0, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
			assertEquals(1, n5.listCallCount);

			n5.list(cachedGroup);
			assertEquals(0, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
			assertEquals(2, n5.listCallCount);

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
			assertEquals(1, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
			assertEquals(2, n5.listCallCount);

			n5.groupExists(nonExistentGroup);
			assertEquals(1, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
			assertEquals(2, n5.listCallCount);

			n5.datasetExists(nonExistentGroup);
			assertEquals(1, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
			assertEquals(2, n5.listCallCount);

			n5.getAttributes(nonExistentGroup);
			assertEquals(1, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
			assertEquals(2, n5.listCallCount);

			n5.list(nonExistentGroup);
			assertEquals(1, n5.existsCallCount);
			assertEquals(0, n5.isGroupCallCount);
			assertEquals(0, n5.isDatasetCallCount);
			assertEquals(0, n5.attrCallCount);
			assertEquals(2, n5.listCallCount);
		}
	}

	protected static class TrackingStorage extends N5KeyValueWriter {

		int attrCallCount = 0;
		int existsCallCount = 0;
		int isGroupCallCount = 0;
		int isGroupAttrCallCount = 0;
		int isDatasetCallCount = 0;
		int isDatasetAttrCallCount = 0;
		int listCallCount = 0;

		public TrackingStorage(final KeyValueAccess keyValueAccess, final String basePath,
				final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws IOException {

			super(keyValueAccess, basePath, gsonBuilder, cacheAttributes);
		}

		public JsonElement getAttributesFromContainer(final String key, final String cacheKey) {
			attrCallCount++;
			return super.getAttributesFromContainer(key, cacheKey);
		}

		public boolean existsFromContainer(final String path, final String cacheKey) {
			existsCallCount++;
			return super.existsFromContainer(path, cacheKey);
		}

		public boolean isGroupFromContainer(final String key) {
			isGroupCallCount++;
			return super.isGroupFromContainer(key);
		}

		public boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes) {
			isGroupAttrCallCount++;
			return super.isGroupFromAttributes(normalCacheKey, attributes);
		}

		public boolean isDatasetFromContainer(final String key) {
			isDatasetCallCount++;
			return super.isDatasetFromContainer(key);
		}

		public boolean isDatasetFromAttributes(final String normalCacheKey, final JsonElement attributes) {
			isDatasetAttrCallCount++;
			return super.isDatasetFromAttributes(normalCacheKey, attributes);
		}

		public String[] listFromContainer(final String key) {
			listCallCount++;
			return super.listFromContainer(key);
		}
	}

}
