package org.janelia.saalfeldlab.n5;

import javax.annotation.Nonnegative;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SplitKeyValueAccessData implements SplitableData {

	private final KeyValueAccess access;
	private final String key;
	private final long offset;
	private final long size;

	public SplitKeyValueAccessData(
			final KeyValueAccess access,
			final String key) throws IOException {
		this.access = access;
		this.key = key;
		this.offset = 0;
		long keySize;
		try {
			/*If the file exists we need to know the real size, in case we need to read the
			 * 	Index N bytes from the end, for example. Potentially we could do this lazily */
			keySize = access.size(key);
		} catch (N5Exception.N5NoSuchKeyException e) {
			keySize = 0; //TODO Caleb: 0? -1? ???
		}
		this.size = keySize;
	}

	public SplitKeyValueAccessData(
			final KeyValueAccess access,
			final String key,
			final long offset,
			final long size){

		this.access = access;
		this.key = key;
		this.offset = offset;
		this.size = size;
	}

	@Override
	public long getOffset() {

		return offset;
	}

	@Override
	public long getSize() {

		return size;
	}

	@Override
	public InputStream newInputStream() throws IOException {
		//TODO Caleb: Should wrap with BoundedInputStream?
		return access.lockForReading(key, offset, size).newInputStream();
	}

	@Override
	public OutputStream newOutputStream() throws IOException {
		//TODO Caleb: If 0 -> -1? to handle non-existent files
		return access.lockForWriting(key, offset, size).newOutputStream();
	}

	@Override
	public SplitableData split(@Nonnegative long offset, long size) {

		return new SplitKeyValueAccessData(access, key, getOffset() + offset, size);
	}
}
