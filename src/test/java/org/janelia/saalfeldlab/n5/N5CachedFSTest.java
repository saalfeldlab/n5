package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;
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
	protected N5Writer createN5Writer() throws IOException {

		return new N5FSWriter(tempN5PathName(), true);
	}

	@Override
	protected N5Writer createN5Writer(final String location) throws IOException {

		return createN5Writer(location, true);
	}

	protected N5Writer createN5Writer(final String location, final boolean cache ) throws IOException {

		return new N5FSWriter(location, cache);
	}

	@Override
	protected N5Writer createN5Writer(final String location, final GsonBuilder gson) throws IOException {

		if (!new File(location).exists()) {
			tmpFiles.add(location);
		}
		return new N5FSWriter(location, gson, true);
	}

	protected N5Reader createN5Reader(final String location, final GsonBuilder gson, final boolean cache) throws IOException {

		return new N5FSReader(location, gson, cache);
	}

	@Test
	public void cacheTest() throws IOException {
		/* Test the cache by setting many attributes, then manually deleting the underlying file.
		* The only possible way for the test to succeed is if it never again attempts to read the file, and relies on the cache. */

//		try (N5KeyValueWriter n5 = (N5KeyValueWriter) createN5Writer()) {
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

//		try (N5KeyValueWriter n5 = new N5FSWriter(tempN5PathName(), false)) {
		try (N5KeyValueWriter n5 = (N5KeyValueWriter)createN5Writer(tempN5PathName(), false)) {
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
	public void cacheGroupDatasetTest() throws IOException {

		final String datasetName = "dd";
		final String groupName = "gg";

		final String tmpPath = tempN5PathName();
		try (N5KeyValueWriter w1 = (N5KeyValueWriter) createN5Writer(tmpPath);
				N5KeyValueWriter w2 = (N5KeyValueWriter) createN5Writer(tmpPath);) {

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

}
