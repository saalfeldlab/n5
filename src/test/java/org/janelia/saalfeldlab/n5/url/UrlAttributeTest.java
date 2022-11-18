package org.janelia.saalfeldlab.n5.url;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.junit.Before;
import org.junit.Test;

public class UrlAttributeTest
{
	N5Reader n5;
	String rootContext = "";
	int[] list;
	HashMap<String,String> obj;
	Set<String> rootKeys;
	
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
	}
	
	@SuppressWarnings( "unchecked" )
	@Test
	public void testRootAttributes()
	{
		try
		{
			// get 
			HashMap<String,Object> everything = n5.getAttribute( "", HashMap.class );
			assertEquals( "2.5.1", (String) everything.get( "n5" ));

			assertEquals( "bar", n5.getAttribute( rootContext, String.class ) );
			assertEquals( list, n5.getAttribute( "", int[].class ) );
			assertEquals( list, n5.getAttribute( "", int[].class ) );

			// list
			assertEquals( list[0], (int)n5.getAttribute( rootContext, Integer.class ) );
			assertEquals( list[1], (int)n5.getAttribute( rootContext, Integer.class ) );
			assertEquals( list[2], (int)n5.getAttribute( rootContext, Integer.class ) );

			assertEquals( list[3], (int)n5.getAttribute( rootContext, Integer.class ) );
			assertEquals( list[3], (int)n5.getAttribute( rootContext, Integer.class ) );
	
			// object
			assertTrue( mapsEqual( obj, n5.getAttribute( rootContext, Map.class ) ) );
			
		}
		catch ( IOException e )
		{
			fail( e.getMessage() );
		}

	}

	public void testAbsolutePathAttributes()
	{
		final String a = "a";
		final String aa = "aa";
		final String aaa = "aaa";
		try
		{
			// name of a
			assertEquals( "name of a from root", a, n5.getAttribute( "", String.class ) );
			assertEquals( "name of a from root", a, n5.getAttribute( "", String.class ) );
			assertEquals( "name of a from a", a, n5.getAttribute( "a", String.class ) );
			assertEquals( "name of a from aa", a, n5.getAttribute( "aa", String.class ) );
			assertEquals( "name of a from aaa", a, n5.getAttribute( "aaa", String.class ) );
			
			// name of aa
			assertEquals( "name of aa from root", aa, n5.getAttribute( "", String.class ) );
			assertEquals( "name of aa from root", aa, n5.getAttribute( "", String.class ) );
			assertEquals( "name of aa from a", aa, n5.getAttribute( "a", String.class ) );
			assertEquals( "name of aa from aa", aa, n5.getAttribute( "aa", String.class ) );
			assertEquals( "name of aa from aa", aa, n5.getAttribute( "aa", String.class ) );
			assertEquals( "name of aa from aaa", aa, n5.getAttribute( "aaa", String.class ) );

			// name of aaa
			assertEquals( "name of aaa from root", aaa, n5.getAttribute( "", String.class ) );
			assertEquals( "name of aaa from root", aaa, n5.getAttribute( "", String.class ) );
			assertEquals( "name of aaa from a", aaa, n5.getAttribute( "a", String.class ) );
			assertEquals( "name of aaa from aa", aaa, n5.getAttribute( "aa", String.class ) );
			assertEquals( "name of aaa from aaa", aaa, n5.getAttribute( "aaa", String.class ) );
			assertEquals( "name of aaa from aaa", aaa, n5.getAttribute( "aaa", String.class ) );
		}
		catch ( IOException e )
		{
			fail( e.getMessage() );
		}
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

}
