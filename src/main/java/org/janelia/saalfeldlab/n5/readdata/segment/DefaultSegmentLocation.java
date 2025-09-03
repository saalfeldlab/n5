package org.janelia.saalfeldlab.n5.readdata.segment;

class DefaultSegmentLocation implements SegmentLocation {

	private final long offset;
	private final long length;

	public DefaultSegmentLocation(final long offset, final long length) {
		this.offset = offset;
		this.length = length;
	}

	@Override
	public long offset() {
		return offset;
	}

	@Override
	public long length() {
		return length;
	}

	@Override
	public String toString() {
		return "SegmentLocation{" +
				"offset=" + offset +
				", length=" + length +
				'}';
	}
}
