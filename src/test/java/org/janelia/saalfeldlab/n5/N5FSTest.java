/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.saalfeldlab.n5;

import com.google.gson.reflect.TypeToken;
import org.janelia.saalfeldlab.n5.url.UrlAttributeTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * Initiates testing of the filesystem-based N5 implementation.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5FSTest extends AbstractN5Test {

	static private String testDirPath;

	static {
		try {
			testDirPath = Files.createTempDirectory("n5-test-").toFile().getCanonicalPath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @throws IOException
	 */
	@Override
	protected N5Writer createN5Writer() throws IOException {

		return new N5FSWriter(testDirPath);
	}



	private class TestData<T> {
		public String key;
		public T value;
		public Class<T> cls;

		public TestData(String key, T value) {

			this.key = key;
			this.value = value;
			this.cls = (Class<T>) value.getClass();
		}
	}

	@Test
	public void testAttributePaths() throws IOException {
		String testGroup = "test";
		n5.createGroup(testGroup);

		final ArrayList<TestData<?>> existingTests = new ArrayList<>();

		Consumer<TestData<?>> addAndTest = testData -> {
			/* test a new value on existing path */
			try {
				n5.setAttribute(testGroup, testData.key, testData.value);
				Assert.assertEquals(testData.value, n5.getAttribute(testGroup, testData.key, testData.cls));
				Assert.assertEquals(testData.value, n5.getAttribute(testGroup, testData.key, TypeToken.get(testData.cls).getType()));

				/* previous values should still be there, but we remove first if the test we just added overwrites. */
				existingTests.removeIf(test -> {
					try {
						final String normalizedTestKey = N5URL.from(null, "", test.key).normalizeAttributePath().replaceAll("^/", "");
						final String normalizedTestDataKey = N5URL.from(null, "", testData.key).normalizeAttributePath().replaceAll("^/", "");
						return normalizedTestKey.equals(normalizedTestDataKey);
					} catch (URISyntaxException e) {
						throw new RuntimeException(e);
					}
				});
				for (TestData<?> test : existingTests) {
					Assert.assertEquals(test.value, n5.getAttribute(testGroup, test.key, test.cls));
					Assert.assertEquals(test.value, n5.getAttribute(testGroup, test.key, TypeToken.get(test.cls).getType()));
				}
				existingTests.add(testData);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		};

		/* Test a new value by path */
		addAndTest.accept(new TestData<>("/a/b/c/key1", "value1"));
		/* test a new value on existing path */
		addAndTest.accept(new TestData<>("/a/b/key2", "value2"));
		/* test replacing an existing value */
		addAndTest.accept(new TestData<>("/a/b/c/key1", "new_value1"));

		/* Test a new value with arrays */
		addAndTest.accept(new TestData<>("/array[0]/b/c/key1", "array_value1"));
		/* test replacing an existing value */
		addAndTest.accept(new TestData<>("/array[0]/b/c/key1", "new_array_value1"));
		/* test a new value on existing path with arrays */
		addAndTest.accept(new TestData<>("/array[0]/d[3]/key2", "array_value2"));
		/* test a new value on existing path with nested arrays */
		addAndTest.accept(new TestData<>("/array[1][2]/[3]key2", "array2_value2"));
		/* test with syntax variants */
		addAndTest.accept(new TestData<>("/array[1][2]/[3]key2", "array3_value3"));
		addAndTest.accept(new TestData<>("array[1]/[2][3]/key2", "array3_value4"));
		addAndTest.accept(new TestData<>("/array/[1]/[2]/[3]/key2", "array3_value5"));

		/* Non String tests */

		addAndTest.accept(new TestData<>("/an/integer/test", 1));
		addAndTest.accept(new TestData<>("/a/double/test", 1.0));
		addAndTest.accept(new TestData<>("/a/float/test", 1.0F));
		addAndTest.accept(new TestData<>("/a/boolean/test", true));



		final UrlAttributeTest.TestDoubles doubles1 = new UrlAttributeTest.TestDoubles( "doubles", "doubles1", new double[] { 5.7, 4.5, 3.4 } );
		final UrlAttributeTest.TestDoubles doubles2 = new UrlAttributeTest.TestDoubles( "doubles", "doubles2", new double[] { 5.8, 4.6, 3.5 } );
		final UrlAttributeTest.TestDoubles doubles3 = new UrlAttributeTest.TestDoubles( "doubles", "doubles3", new double[] { 5.9, 4.7, 3.6 } );
		final UrlAttributeTest.TestDoubles doubles4 = new UrlAttributeTest.TestDoubles( "doubles", "doubles4", new double[] { 5.10, 4.8, 3.7 } );
		addAndTest.accept(new TestData<>("/doubles[1]", doubles1));
		addAndTest.accept(new TestData<>("/doubles[2]", doubles2));
		addAndTest.accept(new TestData<>("/doubles[3]", doubles3));
		addAndTest.accept(new TestData<>("/doubles[4]", doubles4));

		/* Test overwrite custom */
		addAndTest.accept(new TestData<>("/doubles[1]", doubles4));

		n5.remove(testGroup);
	}

	@Test
	public void testRootLeaves() throws IOException {
		//TODO: Currently, this fails if you try to overwrite an existing root; should we support that?
		// In general, the current situation is such that you can only replace leaf nodes;

		final ArrayList<TestData<?>> tests = new ArrayList<>();
		tests.add(new TestData<>("", "empty_root"));
		tests.add(new TestData<>("/", "replace_empty_root"));
		tests.add(new TestData<>("[0]", "array_root"));

		for (TestData<?> testData : tests) {
			final String canonicalPath = Files.createTempDirectory("root-leaf-test-").toFile().getCanonicalPath();
			try (final N5FSWriter writer = new N5FSWriter(canonicalPath)) {
				writer.setAttribute(groupName, testData.key, testData.value);
				Assert.assertEquals(testData.value, writer.getAttribute(groupName, testData.key, testData.cls));
				Assert.assertEquals(testData.value, writer.getAttribute(groupName, testData.key, TypeToken.get(testData.cls).getType()));
			}
		}
	}
}
