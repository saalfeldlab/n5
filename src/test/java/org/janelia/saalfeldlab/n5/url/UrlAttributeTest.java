package org.janelia.saalfeldlab.n5.url;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UrlAttributeTest
{
	N5Reader n5;
	String rootContext = "";
	int[] list;
	HashMap<String,String> obj;
	Set<String> rootKeys;

	TestInts testObjInts;
	TestDoubles testObjDoubles;

	@Before
	public void before()
	{
		try
		{
			n5 = new N5FSReader( "src/test/resources/url/urlAttributes.n5" );
			rootContext = "";
			list = new int[] { 0, 1, 2, 3 };

			obj = new HashMap<>();
			obj.put( "a", "aa" );
			obj.put( "b", "bb" );
			rootKeys = new HashSet<>();
			rootKeys.addAll( Stream.of("n5", "foo", "list", "object" ).collect( Collectors.toList() ) );
		}
		catch ( IOException e )
		{
			e.printStackTrace();
		}

		testObjInts = new TestInts( "ints", "intsName", new int[] { 5, 4, 3 } );
		testObjDoubles = new TestDoubles( "doubles", "doublesName", new double[] { 5.5, 4.4, 3.3 } );
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRootAttributes() throws URISyntaxException, IOException {
		// get
		Map<String, Object> everything = n5.getAttribute(new N5URL(""), Map.class);
		assertEquals("empty url", "2.5.1", (String)everything.get("n5"));

		Map<String, Object> everything2 = n5.getAttribute(new N5URL("#/"), Map.class);
		assertEquals("root attribute", "2.5.1", (String)everything2.get("n5"));

		assertEquals("url to attribute", "bar", n5.getAttribute(new N5URL("#foo"), String.class));
		assertEquals("url to attribute absolute", "bar", n5.getAttribute(new N5URL("#/foo"), String.class));

		assertEquals("#foo", "bar", n5.getAttribute(new N5URL("#foo"), String.class));
		assertEquals("#/foo", "bar", n5.getAttribute(new N5URL("#/foo"), String.class));
		assertEquals("?#foo", "bar", n5.getAttribute(new N5URL("?#foo"), String.class));
		assertEquals("?#/foo", "bar", n5.getAttribute(new N5URL("?#/foo"), String.class));
		assertEquals("?/#/foo", "bar", n5.getAttribute(new N5URL("?/#/foo"), String.class));
		assertEquals("?/.#/foo", "bar", n5.getAttribute(new N5URL("?/.#/foo"), String.class));
		assertEquals("?./#/foo", "bar", n5.getAttribute(new N5URL("?./#/foo"), String.class));
		assertEquals("?.#foo", "bar", n5.getAttribute(new N5URL("?.#foo"), String.class));
		assertEquals("?/a/..#foo", "bar", n5.getAttribute(new N5URL("?/a/..#foo"), String.class));
		assertEquals("?/a/../.#foo", "bar", n5.getAttribute(new N5URL("?/a/../.#foo"), String.class));

		Assert.assertArrayEquals( "url list", list, n5.getAttribute(new N5URL("#list"), int[].class));

		// list
		assertEquals("url list[0]", list[0], (int)n5.getAttribute(new N5URL("#list[0]"), Integer.class));
		assertEquals("url list[1]", list[1], (int)n5.getAttribute(new N5URL("#list[1]"), Integer.class));
		assertEquals("url list[2]", list[2], (int)n5.getAttribute(new N5URL("#list[2]"), Integer.class));

		assertEquals("url list[3]", list[3], (int)n5.getAttribute(new N5URL("#list[3]"), Integer.class));
		assertEquals("url list/[3]", list[3], (int)n5.getAttribute(new N5URL("#list/[3]"), Integer.class));
		assertEquals("url list//[3]", list[3], (int)n5.getAttribute(new N5URL("#list//[3]"), Integer.class));
		assertEquals("url //list//[3]", list[3], (int)n5.getAttribute(new N5URL("#//list//[3]"), Integer.class));
		assertEquals("url //list//[3]//", list[3], (int)n5.getAttribute(new N5URL("#//list////[3]//"), Integer.class));

		// object
		assertTrue("url object", mapsEqual(obj, n5.getAttribute(new N5URL("#object"), Map.class)));
		assertEquals("url object/a", "aa", n5.getAttribute(new N5URL("#object/a"), String.class));
		assertEquals("url object/b", "bb", n5.getAttribute(new N5URL("#object/b"), String.class));

		// failures
		// or assertThrows?
		// Currently, they pass, but only because we define the test by the current results. We should discuss what we want though.
		assertThrows("url to attribute", NoSuchFileException.class, () -> n5.getAttribute(new N5URL("file://garbage.n5?trash#foo"), String.class));
		assertEquals("url to attribute", "bar", n5.getAttribute(new N5URL("file://garbage.n5?/#foo"), String.class));

	}

	@Test
	public void testPathAttributes() throws URISyntaxException, IOException {

		final String a = "a";
		final String aa = "aa";
		final String aaa = "aaa";

		final N5URL aUrl = new N5URL("?/a");
		final N5URL aaUrl = new N5URL("?/a/aa");
		final N5URL aaaUrl = new N5URL("?/a/aa/aaa");

			// name of a
			assertEquals( "name of a from root", a, n5.getAttribute( new N5URL("?/a#name"), String.class ) );
			assertEquals( "name of a from root", a, n5.getAttribute( new N5URL("?a#name"), String.class ) );
			assertEquals( "name of a from a", a, n5.getAttribute( aUrl.getRelative( new N5URL("?/a#name")), String.class ));
		assertEquals( "name of a from aa", a, n5.getAttribute(aaUrl.getRelative("?..#name"), String.class ));
			assertEquals( "name of a from aaa", a, n5.getAttribute( aaaUrl.getRelative( new N5URL("?../..#name")), String.class ));

		// name of aa
		assertEquals("name of aa from root", aa, n5.getAttribute(new N5URL("?/a/aa#name"), String.class));
		assertEquals("name of aa from root", aa, n5.getAttribute(new N5URL("?a/aa#name"), String.class));
		assertEquals("name of aa from a", aa, n5.getAttribute(aUrl.getRelative("?aa#name"), String.class));

		assertEquals("name of aa from aa", aa, n5.getAttribute(aaUrl.getRelative("?./#name"), String.class));

		assertEquals("name of aa from aa", aa, n5.getAttribute(aaUrl.getRelative("#name"), String.class));
		assertEquals("name of aa from aaa", aa, n5.getAttribute(aaaUrl.getRelative("?..#name"), String.class));

		// name of aaa
		assertEquals("name of aaa from root", aaa, n5.getAttribute(new N5URL("?/a/aa/aaa#name"), String.class));
		assertEquals("name of aaa from root", aaa, n5.getAttribute(new N5URL("?a/aa/aaa#name"), String.class));
		assertEquals("name of aaa from a", aaa, n5.getAttribute(aUrl.getRelative("?aa/aaa#name"), String.class));
		assertEquals("name of aaa from aa", aaa, n5.getAttribute(aaUrl.getRelative("?aaa#name"), String.class));
		assertEquals("name of aaa from aaa", aaa, n5.getAttribute(aaaUrl.getRelative("#name"), String.class));

		assertEquals("name of aaa from aaa", aaa, n5.getAttribute(aaaUrl.getRelative("?./#name"), String.class));
	}

	@Test
	public void testPathObject() throws IOException, URISyntaxException
	{
		final TestInts ints = n5.getAttribute( new N5URL( "?objs#intsKey" ), TestInts.class );
		assertEquals( testObjInts.name, ints.name );
		assertEquals( testObjInts.type, ints.type );
		assertArrayEquals( testObjInts.t(), ints.t() );

		final TestDoubles doubles = n5.getAttribute( new N5URL( "?objs#doublesKey" ), TestDoubles.class );
		assertEquals( testObjDoubles.name, doubles.name );
		assertEquals( testObjDoubles.type, doubles.type );
		assertArrayEquals( testObjDoubles.t(), doubles.t(), 1e-9 );
	}

	private <K,V> boolean mapsEqual( Map<K,V> a, Map<K,V> b )
	{
		if( ! a.keySet().equals( b.keySet() ))
			return false;

		for( K k : a.keySet() )
		{
			if( ! a.get( k ).equals( b.get( k ) ))
				return false;
		}

		return true;
	}

	private static class TestObject< T >
	{
		String type;
		String name;
		T t;

		public TestObject( String type, String name, T t )
		{
			this.name = name;
			this.type = type;
			this.t = t;
		}
		public T t() { return t; }
	}

	private static class TestDoubles extends TestObject< double[] >
	{
		public TestDoubles( String type, String name, double[] t )
		{
			super( type, name, t );
		}
	}

	private static class TestInts extends TestObject< int[] >
	{
		public TestInts( String type, String name, int[] t )
		{
			super( type, name, t );
		}
	}

}
