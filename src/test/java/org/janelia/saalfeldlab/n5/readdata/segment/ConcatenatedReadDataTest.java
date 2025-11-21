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
package org.janelia.saalfeldlab.n5.readdata.segment;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentedReadData.SegmentsAndData;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConcatenatedReadDataTest {

	private final byte[] data = new byte[100];

	@Before
	public void fillData() {

		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) i;
		}
	}

	/**
	 * Create a SegmentedReadData with segments at (10,l=30), (0,l=10), and (40,l=20).
	 * The returned ReadData knows its length.
	 * <p>
	 * <pre>
	 * [0.....10.....20.....30.....40.....50.....60.....70.....80.....90.....]
	 * [(-s1-)(---------s0--------)(-----s2-----)............................]
	 * </pre>
	 */
	private SegmentsAndData createKnownLength() {

		return SegmentedReadData.wrap(
				ReadData.from(data),
				Range.at(10, 30),
				Range.at(0, 10),
				Range.at(40, 20));
	}

	/**
	 * Create a SegmentedReadData with one segment spanning it completely.
	 * The returned ReadData doesn't know its length.
	 * <p>
	 * <pre>
	 * [0.....10.....20.....30.....40.....50.....60.....70.....80.....90.....]
	 * [(------------------------------s3-----------------------------------)]
	 * </pre>
	 */
	private SegmentsAndData createUnknownLength() {

		final SegmentedReadData srd = SegmentedReadData.wrap(
				ReadData.from(
						new ByteArrayInputStream(data)));

		return new SegmentsAndData() {

			@Override
			public List<Segment> segments() {
				return Collections.singletonList(srd.segments().get(0));
			}

			@Override
			public SegmentedReadData data() {
				return srd;
			}
		};
	}


	/**
	 * Create slices of known length and unknown length SegmentedReadData, and concatenate.
	 * Take slice (0,l=40)
	 * <p>
	 * <pre>
	 *
	 * KNOWN_LENGTH:
	 * [0.....10.....20.....30.....40.....50.....60.....70.....80.....90.....]
	 * [(-s1-)(---------s0--------)(-----s2-----)............................]
	 *
	 * SLICE_0
	 * [(-s1-)(---------s0--------)]
	 *
	 *                            SLICE_1
	 *                            [(-----s2-----)]
	 *
	 * UNKNOWN_LENGTH:
	 * [0.....10.....20.....30.....40.....50.....60.....70.....80.....90.....]
	 * [(------------------------------s3-----------------------------------)]
	 *
	 * CONCATENATED:
	 * [-------- SLICE_0 ----------][- UNKNOWN_LENGTH -][-- SLICE_1 ---]
	 * [(-s1-)(---------s0--------)][(--...--s3--...--)][(-----s2-----)]
	 *
	 * </pre>
	 */
	private SegmentsAndData createConcatenate() {

		final SegmentsAndData segmentsAndData0 = createKnownLength();
		final SegmentedReadData r0 = segmentsAndData0.data();

		final SegmentsAndData segmentsAndData1 = createUnknownLength();
		final SegmentedReadData r1 = segmentsAndData1.data();

		final List<Segment> segments = new ArrayList<>();
		segments.addAll(segmentsAndData0.segments());
		segments.addAll(segmentsAndData1.segments());

		final List<SegmentedReadData> datas = new ArrayList<>();
		datas.add(r0.slice(0,40));
		datas.add(r1);
		datas.add(r0.slice(segments.get(2)));
		final SegmentedReadData concatenated = SegmentedReadData.concatenate(datas);

		return new SegmentsAndData() {

			@Override
			public List<Segment> segments() {
				return segments;
			}

			@Override
			public SegmentedReadData data() {
				return concatenated;
			}
		};
	}

	/**
	 * Check that segments in the concatenated ReadData are at the expected locations.
	 * The segments should be laid out like this:
	 * <p>
	 * <pre>
	 * CONCATENATED:
	 * [0.....10.....20.....30.....40...             ...140.....150.....]
	 * [-------- SLICE_0 ----------][- UNKNOWN_LENGTH -][--- SLICE_1 ---]
	 * [(-s1-)(---------s0--------)][(--...--s3--...--)][(------s2-----)]
	 * </pre>
	 * <p>
	 */
	private static void checkSegmentLocations(final SegmentsAndData segmentsAndData) {
		checkSegmentRange(segmentsAndData,1, Range.at(0, 10));
		checkSegmentRange(segmentsAndData,0, Range.at(10, 30));
		checkSegmentRange(segmentsAndData,3, Range.at(40, 100));
		checkSegmentRange(segmentsAndData,2, Range.at(140, 20));
	}

	private static void checkSegmentRange(final SegmentsAndData data, final int segmentIndex, final Range expectedLocation)
	{
		final Range location = data.data().location(data.segments().get(segmentIndex));
		assertEquals(expectedLocation.offset(), location.offset());
		assertEquals(expectedLocation.length(), location.length());
	}

	// A concatenated ReadData containing unknown-length parts requires
	// either materialize() or writeTo(OutputStream) to make those lengths
	// known. Otherwise, trying to get segment locations from the
	// concatenated ReadData fails with an IllegalStateException.

	@Test(expected = IllegalStateException.class)
	public void testConcatenateUnmaterialized() {

		final SegmentsAndData segmentsAndData = createConcatenate();
		final SegmentedReadData c = segmentsAndData.data();
		final List<Segment> s = segmentsAndData.segments();
		System.out.println(c.location(s.get(0)));
	}

	// Calling materialize() on the concatenated ReadData makes sure that all
	// segment offsets are known.

	@Test
	public void testConcatenateMaterialize() {

		final SegmentsAndData segmentsAndData = createConcatenate();
		segmentsAndData.data().materialize();
		checkSegmentLocations(segmentsAndData);
	}

	// Calling writeTo(OutputStream) on the concatenated ReadData makes sure
	// that all segment offsets are known.

	@Test
	public void testConcatenateWriteTo() {

		final SegmentsAndData segmentsAndData = createConcatenate();
		segmentsAndData.data().writeTo(new ByteArrayOutputStream());
		checkSegmentLocations(segmentsAndData);
	}
}
