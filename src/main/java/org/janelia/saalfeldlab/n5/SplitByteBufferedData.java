package org.janelia.saalfeldlab.n5;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SplitByteBufferedData implements SplitableData {

	private byte[] sharedHierarchyData;
	private int maxCount = 0;

	private final AccessibleByteArrayOutputStream data;
	private long offset;

	public SplitByteBufferedData() {
		this(32);
	}

	public SplitByteBufferedData(final int initialSize) {

		this(new byte[initialSize]);
	}

	public SplitByteBufferedData(final int initialSize, final long offset) {

		this(new byte[initialSize], offset);
	}

	public SplitByteBufferedData(final byte[] initialBuffer) {

		this(initialBuffer, 0);
	}

	public SplitByteBufferedData(final byte[] initialBuffer, final long offset) {

		this.offset = offset;
		this.sharedHierarchyData = initialBuffer;
		this.data = new AccessibleByteArrayOutputStream(this::getSharedBuffer, this::setSharedBuffer, this::getMaxCount, this::setMaxCount);
		data.setCount((int)offset);
		maxCount = initialBuffer.length;
	}

	private SplitByteBufferedData(final AccessibleByteArrayOutputStream splitData, final long offset, final long size) {

		this.offset = offset;
		this.data = splitData;
		data.setCount((int)offset);
	}

	private byte[] getSharedBuffer() {
		return sharedHierarchyData;
	}

	private void setSharedBuffer(byte[] buffer) {
		sharedHierarchyData = buffer;
	}

	private void setMaxCount(int count) {
		if (count > maxCount)
			maxCount = count;
	}

	private int getMaxCount() {
		return maxCount;
	}

	@Override
	public long getOffset() {

		return offset;
	}

	@Override
	public long getSize() {

		return getMaxCount() - getOffset();
	}

	@Override
	public InputStream newInputStream() {

		final byte[] sharedBuffer = data.getBuf();
		return new ByteArrayInputStream(sharedBuffer, (int)offset, (int)(data.getMaxCount.get() - offset));
	}

	@Override
	public OutputStream newOutputStream()  {

		return data;
	}

	@Override
	public SplitableData split(long offset, long size) {

		final AccessibleByteArrayOutputStream newData = new AccessibleByteArrayOutputStream(
				data.getSharedBuffer, data.setSharedBuffer,
				data.getMaxCount, data.setMaxCount
				);
		return new SplitByteBufferedData(newData, offset, size);
	}

	private static class AccessibleByteArrayOutputStream extends ByteArrayOutputStream {

		private final Supplier<byte[]> getSharedBuffer;
		private final Consumer<byte[]> setSharedBuffer;
		private final Supplier<Integer> getMaxCount;
		private final Consumer<Integer> setMaxCount;

		AccessibleByteArrayOutputStream(
				final Supplier<byte[]> getSharedBuffer,
				final Consumer<byte[]> setSharedBuffer,
				final Supplier<Integer> getMaxCount,
				final Consumer<Integer> setMaxCount) {

			this.getSharedBuffer = getSharedBuffer;
			this.setSharedBuffer = setSharedBuffer;
			this.getMaxCount = getMaxCount;
			this.setMaxCount = setMaxCount;
		}

		byte[] getBuf() {
			return getSharedBuffer.get();
		}

		void setCount(int count) {
			this.count = count;
		}

		@Override public synchronized void write(int b) {

			final byte[] sharedBufBeforeWrite = getSharedBuffer.get();

			if (buf != sharedBufBeforeWrite) buf = sharedBufBeforeWrite;
			super.write(b);
			if (buf != sharedBufBeforeWrite) setSharedBuffer.accept(buf);
			setMaxCount.accept(count);
		}

		@Override public synchronized void write(byte[] b, int off, int len) {

			final byte[] sharedBufBeforeWrite = getSharedBuffer.get();
			if (buf != sharedBufBeforeWrite) buf = sharedBufBeforeWrite;
			super.write(b, off, len);
			if (buf != sharedBufBeforeWrite) setSharedBuffer.accept(buf);
			setMaxCount.accept(count);
		}
	}
}
