/**
 *
 */
package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.assertArrayEquals;

import java.nio.file.FileSystems;
import java.nio.file.Paths;
import org.junit.Test;


/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class FileSystemKeyValueAccessTest {

	private static String root = FileSystems.getDefault().getRootDirectories().iterator().next().toString();
	private static String separator = FileSystems.getDefault().getSeparator();
	private static String[] testPaths = new String[]{
			Paths.get(root, "test", "path", "file").toString(),
			Paths.get("test", "path", "file").toString(),
			Paths.get(root, "test", "path", "file", separator).toString(),
			Paths.get("test", "path", "file", separator).toString(),
			Paths.get(root, "file").toString(),
			Paths.get("file").toString(),
			Paths.get(root, "file", separator).toString(),
			Paths.get("file", separator).toString(),
			Paths.get(root).toString(),
			Paths.get("").toString()
	};

	private static String[][] testPathComponents = new String[][] {
			{root, "test", "path", "file"},
			{"test", "path", "file"},
			{root, "test", "path", "file"},
			{"test", "path", "file"},
			{root, "file"},
			{"file"},
			{root, "file"},
			{"file"},
			{root},
			{""}
	};

	@Test
	public void testComponents() {

		final FileSystemKeyValueAccess access = new FileSystemKeyValueAccess(FileSystems.getDefault());

		for (int i = 0; i < testPaths.length; ++i) {

			final String[] components = access.components(testPaths[i]);

			assertArrayEquals(testPathComponents[i], components);
		}
	}
}
