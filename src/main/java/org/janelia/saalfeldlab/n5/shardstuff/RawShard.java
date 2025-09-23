package org.janelia.saalfeldlab.n5.shardstuff;

import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.segment.Segment;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentedReadData;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.NDArray;

public class RawShard {

	private final SegmentedReadData sourceData;

	private final NDArray<Segment> index;

	RawShard(final int[] size) {
		sourceData = null;
		index = new NDArray<>(size, Segment[]::new);
	}

	RawShard(final SegmentedReadData sourceData, final NDArray<Segment> index) {
		this.sourceData = sourceData;
		this.index = index;
	}

	RawShard(final ShardIndex.SegmentIndexAndData segmentIndexAndData) {
		this(segmentIndexAndData.data(), segmentIndexAndData.index());
	}

	/**
	 * The ReadData from which the shard was constructed, or {@code null}
	 * for a new empty shard.
	 */
	public SegmentedReadData sourceData() {
		return sourceData;
	}

	/**
	 * Maps grid position of shard elements to {@link Segment}s.
	 */
	public NDArray<Segment> index() {
		return index;
	}

	public ReadData getElementData(final long[] pos) {
		final Segment segment = index.get(pos);
		return segment == null ? null : segment.source().slice(segment);
	}

	public void setElementData(final ReadData data, final long[] pos) {
		final Segment segment = data == null ? null : SegmentedReadData.wrap(data).segments().get(0);
		index.set(segment, pos);
	}
}
