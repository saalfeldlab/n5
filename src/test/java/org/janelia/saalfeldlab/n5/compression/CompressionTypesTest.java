/**
 *
 */
package org.janelia.saalfeldlab.n5.compression;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.janelia.saalfeldlab.n5.CompressionAdapter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Stephan Saalfeld
 *
 */
public class CompressionTypesTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeAll
	public static void setUpGlobal() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterAll
	public static void tearDownGlobal() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
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
