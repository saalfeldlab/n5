/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.segment.Segment;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentedReadData;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.NDArray;

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
	 * The ReadData from which the shard was constructed, or {@code null} for a
	 * new empty shard.
	 * 
	 * @return this shard's source ReadData, or null.
	 */
	public SegmentedReadData sourceData() {
		return sourceData;
	}

	/**
	 * Maps grid position of shard elements to {@link Segment}s that give the
	 * byte range for the blocks in this shard.
	 * 
	 * @return an NDArray of segments
	 */
	public NDArray<Segment> index() {
		return index;
	}

	public boolean isEmpty() {
		return index().allElementsNull();
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
