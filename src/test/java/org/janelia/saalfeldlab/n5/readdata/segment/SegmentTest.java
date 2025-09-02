package org.janelia.saalfeldlab.n5.readdata.segment;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentStuff.SegmentLocation;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentStuff.SegmentedReadData;
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

		final SegmentLocation[] locations = {
				SegmentLocation.at(0, 10),
				SegmentLocation.at(10, 10),
				SegmentLocation.at(40, 20)};
		final SegmentedReadData r = SegmentedReadData.wrap(readData, locations);
		assertEquals(3, r.segments().size());

		final SegmentLocation l0 = r.location(r.segments().get(0));
		assertEquals(0, l0.offset());
		assertEquals(10, l0.length());

		final SegmentLocation l1 = r.location(r.segments().get(1));
		assertEquals(10, l1.offset());
		assertEquals(10, l1.length());

		final SegmentLocation l2 = r.location(r.segments().get(2));
		assertEquals(40, l2.offset());
		assertEquals(20, l2.length());
	}

	@Test
	public void testSlice() {

		final SegmentLocation[] locations = {
				SegmentLocation.at(0, 10),
				SegmentLocation.at(10, 10),
				SegmentLocation.at(40, 20)};
		final SegmentedReadData r = SegmentedReadData.wrap(readData, locations);
		final SegmentedReadData s = r.slice(10, 60);
		assertEquals(60, s.length());

		assertEquals(2, s.segments().size());

		final SegmentLocation l0 = s.location(s.segments().get(0));
		assertEquals(0, l0.offset());
		assertEquals(10, l0.length());

		final SegmentLocation l1 = s.location(s.segments().get(1));
		assertEquals(30, l1.offset());
		assertEquals(20, l1.length());
	}

	@Test
	public void testSliceSegment() {

		final SegmentLocation[] locations = {
				SegmentLocation.at(0, 10),
				SegmentLocation.at(10, 10),
				SegmentLocation.at(40, 20)};
		final SegmentedReadData r = SegmentedReadData.wrap(readData, locations);
		final SegmentedReadData s = r.slice(r.segments().get(2));
		assertEquals(20, s.length());

		assertEquals(1, s.segments().size());

		final SegmentLocation l0 = s.location(s.segments().get(0));
		assertEquals(0, l0.offset());
		assertEquals(20, l0.length());
	}

	@Test
	public void testWrapFully() {
		final SegmentedReadData r = SegmentedReadData.wrap(readDataUnknownLength);
		assertEquals(1, r.segments().size());

		final SegmentLocation l0 = r.location(r.segments().get(0));
		assertEquals(0, l0.offset());
		assertEquals(-1, l0.length());

		final SegmentedReadData m = r.materialize();
		final SegmentLocation l0m = r.location(r.segments().get(0));
		assertEquals(0, l0m.offset());
		assertEquals(100, l0m.length());

		final SegmentLocation l0m2 = m.location(m.segments().get(0));
		assertEquals(0, l0m2.offset());
		assertEquals(100, l0m2.length());
	}

	@Test
	public void testSliceFullyWrapped() {
		final SegmentedReadData r = SegmentedReadData.wrap(readDataUnknownLength);
		final SegmentedReadData s = r.slice(0, 100);
		assertEquals(100, s.length());

		assertEquals(1, s.segments().size());

		final SegmentLocation l0 = s.location(s.segments().get(0));
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

		final SegmentLocation l0 = s.location(s.segments().get(0));
		assertEquals(0, l0.offset());
		assertEquals(-1, l0.length());
	}



}
