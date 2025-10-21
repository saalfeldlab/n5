package org.janelia.saalfeldlab.n5.readdata.segment;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConcatenateTest {

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
	public void testConcatenate() {

		final Range[] locations = {
				Range.at(10, 30),
				Range.at(0, 10),
				Range.at(40, 20)};
		final SegmentedReadData.SegmentsAndData segmentsAndData0 = SegmentedReadData.wrap(readData, locations);
		final SegmentedReadData r0 = segmentsAndData0.data();
		final List<Segment> segments0 = segmentsAndData0.segments();

		final SegmentedReadData r1 = SegmentedReadData.wrap(readDataUnknownLength);
		assertEquals(1, r1.segments().size());
		final List<Segment> segments1 = Collections.singletonList(r1.segments().get(0));

		final List<SegmentedReadData> datas = new ArrayList<>();
		datas.add(r0.slice(0,40));
		datas.add(r1);
		datas.add(r0.slice(segments0.get(2)));
		final SegmentedReadData c = SegmentedReadData.concatenate(datas);

		// TODO: create individual tests:
		//       Both materialize() and writeTo(OutputStream) should ensure that all SegmentLocations are known
		//       Otherwise we expect IllegalStateException
		c.materialize();
//		c.writeTo(new ByteArrayOutputStream());

//		for (Segment segment : segments0) {
//			System.out.println("c.location(segment) = " + c.location(segment));
//		}
//		System.out.println("c.location(segment) = " + c.location(segments1.get(0)));

		final Range l1 = c.location(segments0.get(1));
		assertEquals(0, l1.offset());
		assertEquals(10, l1.length());

		final Range l0 = c.location(segments0.get(0));
		assertEquals(10, l0.offset());
		assertEquals(30, l0.length());

		final Range l3 = c.location(segments1.get(0));
		assertEquals(40, l3.offset());
		assertEquals(100, l3.length());

		final Range l2 = c.location(segments0.get(2));
		assertEquals(140, l2.offset());
		assertEquals(20, l2.length());


	}

}
