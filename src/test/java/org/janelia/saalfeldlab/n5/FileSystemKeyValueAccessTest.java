/**
 *
 */
package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.assertArrayEquals;

import java.nio.file.FileSystems;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;


/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class FileSystemKeyValueAccessTest {

	private static String[] testPaths = new String[] {
			"/test/path/file",
			"test/path/file",
			"/test/path/file/",
			"test/path/file/",
			"/file",
			"file",
			"/file/",
			"file/",
			"/",
			""
//			"",
//			Paths.get("C:", "test", "path", "file").toString()
	};

	private static String[][] testPathComponents = new String[][] {
			{"/", "test", "path", "file"},
			{"test", "path", "file"},
			{"/", "test", "path", "file"},
			{"test", "path", "file"},
			{"/", "file"},
			{"file"},
			{"/", "file"},
			{"file"},
			{"/"},
			{""}
//			{""},
//			{"C:", "test", "path", "file"}
	};

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {}

	@Test
	public void testComponents() {

		final FileSystemKeyValueAccess access = new FileSystemKeyValueAccess(FileSystems.getDefault());

		for (int i = 0; i <  testPaths.length; ++i) {

			System.out.println(String.format("%d: %s -> %s", i, testPaths[i], Arrays.toString(access.components(testPaths[i]))));

			assertArrayEquals(testPathComponents[i], access.components(testPaths[i]));
		}
	}
}
