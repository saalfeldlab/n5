package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class N5CachedFSTest extends N5FSTest {

	@Override protected N5Writer createN5Writer() throws IOException {

		return new N5FSWriter(tempN5PathName(), true);
	}

	@Override protected N5Writer createN5Writer(String location, GsonBuilder gson) throws IOException {

		if (!new File(location).exists()) {
			tmpFiles.add(location);
		}
		return new N5FSWriter(location, gson, true);
	}

	@Override protected N5Reader createN5Reader(String location, GsonBuilder gson) throws IOException {

		return new N5FSReader(location, gson, true);
	}

	@Test
	public void cacheTest() throws IOException {
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

		try (N5KeyValueWriter n5 = new N5FSWriter(tempN5PathName(), false)) {
			final String cachedGroup = "cachedGroup";
			final String attributesPath = n5.attributesPath(cachedGroup);

			final ArrayList<TestData<?>> tests = new ArrayList<>();
			n5.createGroup(cachedGroup);
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/b/c", 100));
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/a[5]", "asdf"));
			addAndTest(n5, tests, new TestData<>(cachedGroup, "a/a[2]", 0));

			Files.delete(Paths.get(attributesPath));
			Assert.assertThrows(AssertionError.class, () -> runTests(n5, tests));
		}
	}
}
