/**
 *
 */
package org.janelia.saalfeldlab.n5.compression;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Stephan Saalfeld
 *
 */
public class CompressionTypesTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

		CompressionAdapter compressionTypes = CompressionAdapter.getJsonAdapter();

		Field field = CompressionAdapter.class.getDeclaredField("compressionConstructors");
		field.setAccessible(true);
		Object value = field.get(compressionTypes);
		MapUtils.verbosePrint(System.out, "", (Map)value);

		field = CompressionAdapter.class.getDeclaredField("compressionParameters");
		field.setAccessible(true);
		value = field.get(compressionTypes);
		MapUtils.verbosePrint(System.out, "", (Map)value);
	}

}
