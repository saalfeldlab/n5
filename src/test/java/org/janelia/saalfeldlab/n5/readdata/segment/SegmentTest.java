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
import java.util.List;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SegmentTest {

	private ReadData readData;

	private ReadData readDataUnknownLength;

	@Before
	public void createReadData() {

		final byte[] data = new byte[100];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) i;
		}
		readData = ReadData.from(data);
		readDataUnknownLength = ReadData.from(new ByteArrayInputStream(data));
	}

	@Test
	public void testWrap() {

		final Range[] locations = {
				Range.at(0, 10),
				Range.at(10, 10),
				Range.at(40, 20)};
		final SegmentedReadData r = SegmentedReadData.wrap(readData, locations).data();
		assertEquals(3, r.segments().size());

		final Range l0 = r.location(r.segments().get(0));
		assertEquals(0, l0.offset());
		assertEquals(10, l0.length());

		final Range l1 = r.location(r.segments().get(1));
		assertEquals(10, l1.offset());
		assertEquals(10, l1.length());

		final Range l2 = r.location(r.segments().get(2));
		assertEquals(40, l2.offset());
		assertEquals(20, l2.length());
	}

	@Test
	public void testWrapOrder() {

		final Range[] locations = {
				Range.at(10, 10),
				Range.at(0, 10),
				Range.at(40, 20)};
		final SegmentedReadData.SegmentsAndData segmentsAndData = SegmentedReadData.wrap(readData, locations);
		final SegmentedReadData r = segmentsAndData.data();
		final List<Segment> segments = segmentsAndData.segments();

		assertEquals(3, segments.size());

		final Range l0 = r.location(segments.get(0));
		assertEquals(10, l0.offset());
		assertEquals(10, l0.length());

		final Range l1 = r.location(segments.get(1));
		assertEquals(0, l1.offset());
		assertEquals(10, l1.length());

		final Range l2 = r.location(segments.get(2));
		assertEquals(40, l2.offset());
		assertEquals(20, l2.length());
	}

	@Test
	public void testSlice() {

		final Range[] locations = {
				Range.at(0, 10),
				Range.at(10, 10),
				Range.at(40, 20)};
		final SegmentedReadData r = SegmentedReadData.wrap(readData, locations).data();
		final SegmentedReadData s = r.slice(10, 60);
		assertEquals(60, s.length());

		assertEquals(2, s.segments().size());

		final Range l0 = s.location(s.segments().get(0));
		assertEquals(0, l0.offset());
		assertEquals(10, l0.length());

		final Range l1 = s.location(s.segments().get(1));
		assertEquals(30, l1.offset());
		assertEquals(20, l1.length());
	}

	@Test
	public void testSliceSegment() {

		final Range[] locations = {
				Range.at(0, 10),
				Range.at(10, 10),
				Range.at(40, 20)};
		final SegmentedReadData r = SegmentedReadData.wrap(readData, locations).data();
		final SegmentedReadData s = r.slice(r.segments().get(2));
		assertEquals(20, s.length());

		assertEquals(1, s.segments().size());

		final Range l0 = s.location(s.segments().get(0));
		assertEquals(0, l0.offset());
		assertEquals(20, l0.length());
	}

	@Test
	public void testPartialSliceSegment() {

		final Range[] locations = {
				Range.at(0, 10),
				Range.at(10, 10),
				Range.at(40, 20)};
		final SegmentedReadData r = SegmentedReadData.wrap(readData, locations).data();
		// slice covers all of second segment, part of first and third
		final SegmentedReadData s = r.slice(9, 15);
		assertEquals(15, s.length());

		assertEquals(1, s.segments().size());

		final Range l0 = s.location(s.segments().get(0));
		assertEquals(1, l0.offset());
		assertEquals(10, l0.length());
	}

	@Test
	public void testWrapFully() {
		final SegmentedReadData r = SegmentedReadData.wrap(readDataUnknownLength);
		assertEquals(1, r.segments().size());

		final Range l0 = r.location(r.segments().get(0));
		assertEquals(0, l0.offset());
		assertEquals(-1, l0.length());

		final SegmentedReadData m = r.materialize();
		final Range l0m = r.location(r.segments().get(0));
		assertEquals(0, l0m.offset());
		assertEquals(100, l0m.length());

		final Range l0m2 = m.location(m.segments().get(0));
		assertEquals(0, l0m2.offset());
		assertEquals(100, l0m2.length());
	}

	@Test
	public void testSliceFullyWrapped() {
		final SegmentedReadData r = SegmentedReadData.wrap(readDataUnknownLength);
		final SegmentedReadData s = r.slice(0, 100);
		assertEquals(100, s.length());

		assertEquals(1, s.segments().size());

		final Range l0 = s.location(s.segments().get(0));
		assertEquals(0, l0.offset());
		assertEquals(100, l0.length());

		final SegmentedReadData s0 = r.slice(0, 99);
		assertEquals(99, s0.length());
		assertEquals(0, s0.segments().size());

		final SegmentedReadData s1 = r.slice(1, 99);
		assertEquals(99, s1.length());
		assertEquals(0, s1.segments().size());
	}

	@Test
	public void testSliceSegmentFullyWrapped() {

		final SegmentedReadData r = SegmentedReadData.wrap(readDataUnknownLength);
		final SegmentedReadData s = r.slice(r.segments().get(0));
		assertEquals(-1, s.length());

		assertEquals(1, s.segments().size());

		final Range l0 = s.location(s.segments().get(0));
		assertEquals(0, l0.offset());
		assertEquals(-1, l0.length());
	}



}
