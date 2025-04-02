package org.janelia.saalfeldlab.n5.url;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URI;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UriAttributeTest {

	N5Reader n5;

	String rootContext = "";

	int[] list;

	HashMap<String, String> obj;

	Set<String> rootKeys;

	TestInts testObjInts;

	TestDoubles testObjDoubles;

	@Before
	public void before() {

		n5 = new N5FSWriter("src/test/resources/url/urlAttributes.n5");
		rootContext = "";
		list = new int[]{0, 1, 2, 3};

		obj = new HashMap<>();
		obj.put("a", "aa");
		obj.put("b", "bb");
		rootKeys = new HashSet<>();
		rootKeys.addAll(Stream.of("n5", "foo", "list", "object").collect(Collectors.toList()));

		testObjInts = new TestInts("ints", "intsName", new int[]{5, 4, 3});
		testObjDoubles = new TestDoubles("doubles", "doublesName", new double[]{5.5, 4.4, 3.3});
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRootAttributes() throws URISyntaxException, IOException {

		// get
		final Map<String, Object> everything = getAttribute(n5, new N5URI(""), Map.class);
		final String version = "2.6.1"; // the n5 version at the time the test
										// data were created
		assertEquals("empty url", version, everything.get("n5"));

		final Map<String, Object> everything2 = getAttribute(n5, new N5URI("#/"), Map.class);
		assertEquals("root attribute", version, everything2.get("n5"));

		assertEquals("url to attribute", "bar", getAttribute(n5, new N5URI("#foo"), String.class));
		assertEquals("url to attribute absolute", "bar", getAttribute(n5, new N5URI("#/foo"), String.class));

		assertEquals("#foo", "bar", getAttribute(n5, new N5URI("#foo"), String.class));
		assertEquals("#/foo", "bar", getAttribute(n5, new N5URI("#/foo"), String.class));
		assertEquals("?#foo", "bar", getAttribute(n5, new N5URI("?#foo"), String.class));
		assertEquals("?#/foo", "bar", getAttribute(n5, new N5URI("?#/foo"), String.class));
		assertEquals("?/#/foo", "bar", getAttribute(n5, new N5URI("?/#/foo"), String.class));
		assertEquals("?/.#/foo", "bar", getAttribute(n5, new N5URI("?/.#/foo"), String.class));
		assertEquals("?./#/foo", "bar", getAttribute(n5, new N5URI("?./#/foo"), String.class));
		assertEquals("?.#foo", "bar", getAttribute(n5, new N5URI("?.#foo"), String.class));
		assertEquals("?/a/..#foo", "bar", getAttribute(n5, new N5URI("?/a/..#foo"), String.class));
		assertEquals("?/a/../.#foo", "bar", getAttribute(n5, new N5URI("?/a/../.#foo"), String.class));

		/* whitespace-encoding-necesary, fragment-only test */
		assertEquals("#f o o", "b a r", getAttribute(n5, new N5URI("#f o o"), String.class));

		Assert.assertArrayEquals("url list", list, getAttribute(n5, new N5URI("#list"), int[].class));

		// list
		assertEquals("url list[0]", list[0], (int)getAttribute(n5, new N5URI("#list[0]"), Integer.class));
		assertEquals("url list[1]", list[1], (int)getAttribute(n5, new N5URI("#list[1]"), Integer.class));
		assertEquals("url list[2]", list[2], (int)getAttribute(n5, new N5URI("#list[2]"), Integer.class));

		assertEquals("url list[3]", list[3], (int)getAttribute(n5, new N5URI("#list[3]"), Integer.class));
		assertEquals("url list/[3]", list[3], (int)getAttribute(n5, new N5URI("#list/[3]"), Integer.class));
		assertEquals("url list//[3]", list[3], (int)getAttribute(n5, new N5URI("#list//[3]"), Integer.class));
		assertEquals("url //list//[3]", list[3], (int)getAttribute(n5, new N5URI("#//list//[3]"), Integer.class));
		assertEquals("url //list//[3]//", list[3], (int)getAttribute(n5, new N5URI("#//list////[3]//"), Integer.class));

		// object
		assertTrue("url object", mapsEqual(obj, getAttribute(n5, new N5URI("#object"), Map.class)));
		assertEquals("url object/a", "aa", getAttribute(n5, new N5URI("#object/a"), String.class));
		assertEquals("url object/b", "bb", getAttribute(n5, new N5URI("#object/b"), String.class));
	}

	@Test
	public void testPathAttributes() throws URISyntaxException, IOException {

		final String a = "a";
		final String aa = "aa";
		final String aaa = "aaa";

		final N5URI aUrl = new N5URI("?/a");
		final N5URI aaUrl = new N5URI("?/a/aa");
		final N5URI aaaUrl = new N5URI("?/a/aa/aaa");

		// name of a
		assertEquals("name of a from root", a, getAttribute(n5, "?/a#name", String.class));
		assertEquals("name of a from root", a, getAttribute(n5, new N5URI("?a#name"), String.class));
		assertEquals("name of a from a", a, getAttribute(n5, aUrl.resolve(new N5URI("?/a#name")), String.class));
		assertEquals("name of a from aa", a, getAttribute(n5, aaUrl.resolve("?..#name"), String.class));
		assertEquals("name of a from aaa", a, getAttribute(n5, aaaUrl.resolve(new N5URI("?../..#name")), String.class));

		// name of aa
		assertEquals("name of aa from root", aa, getAttribute(n5, new N5URI("?/a/aa#name"), String.class));
		assertEquals("name of aa from root", aa, getAttribute(n5, new N5URI("?a/aa#name"), String.class));
		assertEquals("name of aa from a", aa, getAttribute(n5, aUrl.resolve("?aa#name"), String.class));

		assertEquals("name of aa from aa", aa, getAttribute(n5, aaUrl.resolve("?./#name"), String.class));

		assertEquals("name of aa from aa", aa, getAttribute(n5, aaUrl.resolve("#name"), String.class));
		assertEquals("name of aa from aaa", aa, getAttribute(n5, aaaUrl.resolve("?..#name"), String.class));

		// name of aaa
		assertEquals("name of aaa from root", aaa, getAttribute(n5, new N5URI("?/a/aa/aaa#name"), String.class));
		assertEquals("name of aaa from root", aaa, getAttribute(n5, new N5URI("?a/aa/aaa#name"), String.class));
		assertEquals("name of aaa from a", aaa, getAttribute(n5, aUrl.resolve("?aa/aaa#name"), String.class));
		assertEquals("name of aaa from aa", aaa, getAttribute(n5, aaUrl.resolve("?aaa#name"), String.class));
		assertEquals("name of aaa from aaa", aaa, getAttribute(n5, aaaUrl.resolve("#name"), String.class));

		assertEquals("name of aaa from aaa", aaa, getAttribute(n5, aaaUrl.resolve("?./#name"), String.class));

		assertEquals("nested list 1", (Integer)1, getAttribute(n5, new N5URI("#nestedList[0][0][0]"), Integer.class));
		assertEquals("nested list 1", (Integer)1, getAttribute(n5, new N5URI("#/nestedList/[0][0][0]"), Integer.class));
		assertEquals(
				"nested list 1",
				(Integer)1,
				getAttribute(n5, new N5URI("#nestedList//[0]/[0]///[0]"), Integer.class));
		assertEquals(
				"nested list 1",
				(Integer)1,
				getAttribute(n5, new N5URI("#/nestedList[0]//[0][0]"), Integer.class));

	}

	private <T> T getAttribute(
			final N5Reader n5,
			final String url1,
			final Class<T> clazz) throws URISyntaxException, IOException {

		return getAttribute(n5, url1, null, clazz);
	}

	private <T> T getAttribute(
			final N5Reader n5,
			final String url1,
			final String url2,
			final Class<T> clazz) throws URISyntaxException, IOException {

		final N5URI n5URL = url2 == null ? new N5URI(url1) : new N5URI(url1).resolve(url2);
		return getAttribute(n5, n5URL, clazz);
	}

	private <T> T getAttribute(
			final N5Reader n5,
			final N5URI url1,
			final Class<T> clazz) throws URISyntaxException, IOException {

		return getAttribute(n5, url1, null, clazz);
	}

	private <T> T getAttribute(
			final N5Reader n5,
			final N5URI url1,
			final String url2,
			final Class<T> clazz) throws URISyntaxException, IOException {

		final N5URI n5URL = url2 == null ? url1 : url1.resolve(url2);
		return n5.getAttribute(n5URL.getGroupPath(), n5URL.getAttributePath(), clazz);
	}

	@Test
	public void testPathObject() throws IOException, URISyntaxException {

		final TestInts ints = getAttribute(n5, new N5URI("?objs#intsKey"), TestInts.class);
		assertEquals(testObjInts.name, ints.name);
		assertEquals(testObjInts.type, ints.type);
		assertArrayEquals(testObjInts.t(), ints.t());

		final TestDoubles doubles = getAttribute(n5, new N5URI("?objs#doublesKey"), TestDoubles.class);
		assertEquals(testObjDoubles.name, doubles.name);
		assertEquals(testObjDoubles.type, doubles.type);
		assertArrayEquals(testObjDoubles.t(), doubles.t(), 1e-9);

		final TestDoubles[] doubleArray = getAttribute(n5, new N5URI("?objs#array"), TestDoubles[].class);
		final TestDoubles doubles1 = new TestDoubles("doubles", "doubles1", new double[]{5.7, 4.5, 3.4});
		final TestDoubles doubles2 = new TestDoubles("doubles", "doubles2", new double[]{5.8, 4.6, 3.5});
		final TestDoubles doubles3 = new TestDoubles("doubles", "doubles3", new double[]{5.9, 4.7, 3.6});
		final TestDoubles doubles4 = new TestDoubles("doubles", "doubles4", new double[]{5.10, 4.8, 3.7});
		final TestDoubles[] expectedDoubles = new TestDoubles[]{doubles1, doubles2, doubles3, doubles4};
		assertArrayEquals(expectedDoubles, doubleArray);

		final String[] stringArray = getAttribute(n5, new N5URI("?objs#String"), String[].class);
		final String[] expectedString = new String[]{"This", "is", "a", "test"};

		final Integer[] integerArray = getAttribute(n5, new N5URI("?objs#Integer"), Integer[].class);
		final Integer[] expectedInteger = new Integer[]{1, 2, 3, 4};

		final int[] intArray = getAttribute(n5, new N5URI("?objs#int"), int[].class);
		final int[] expectedInt = new int[]{1, 2, 3, 4};
		assertArrayEquals(expectedInt, intArray);
	}

	private <K, V> boolean mapsEqual(final Map<K, V> a, final Map<K, V> b) {

		if (!a.keySet().equals(b.keySet()))
			return false;

		for (final K k : a.keySet()) {
			if (!a.get(k).equals(b.get(k)))
				return false;
		}

		return true;
	}

	private static class TestObject<T> {

		String type;

		String name;

		T t;

		public TestObject(final String type, final String name, final T t) {

			this.name = name;
			this.type = type;
			this.t = t;
		}

		public T t() {

			return t;
		}

		@Override
		public boolean equals(final Object obj) {

			if (obj.getClass() == this.getClass()) {
				final TestObject<?> otherTestObject = (TestObject<?>)obj;
				return Objects.equals(name, otherTestObject.name)
						&& Objects.equals(type, otherTestObject.type)
						&& Objects.equals(t, otherTestObject.t);
			}
			return false;
		}
	}

	public static class TestDoubles extends TestObject<double[]> {

		public TestDoubles(final String type, final String name, final double[] t) {

			super(type, name, t);
		}

		@Override
		public boolean equals(final Object obj) {

			if (obj.getClass() == this.getClass()) {
				final TestDoubles otherTestObject = (TestDoubles)obj;
				return Objects.equals(name, otherTestObject.name)
						&& Objects.equals(type, otherTestObject.type)
						&& Arrays.equals(t, otherTestObject.t);
			}
			return false;
		}
	}

	private static class TestInts extends TestObject<int[]> {

		public TestInts(final String type, final String name, final int[] t) {

			super(type, name, t);
		}

		@Override
		public boolean equals(final Object obj) {

			if (obj.getClass() == this.getClass()) {
				final TestInts otherTestObject = (TestInts)obj;
				return Objects.equals(name, otherTestObject.name)
						&& Objects.equals(type, otherTestObject.type)
						&& Arrays.equals(t, otherTestObject.t);
			}
			return false;
		}

	}

}
