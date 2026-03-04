package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;

public class RootedKvaPlayground {

	public static void main(String[] args) {

		final String basePath = "/Users/pietzsch/workspace/data/111010_weber_full.n5";
		final GsonBuilder gsonBuilder = new GsonBuilder();
		final boolean cacheMeta = false;


		final N5Reader n5 =  new N5FSReader(basePath, gsonBuilder, cacheMeta);

//		final DatasetAttributes attr = n5.getDatasetAttributes("t00001/s00");
		final DatasetAttributes attr0 = n5.getDatasetAttributes("t00001/s00/s0");
		final DatasetAttributes attr1 = n5.getDatasetAttributes("t00001/s00/s1");
		final DatasetAttributes attr2 = n5.getDatasetAttributes("t00001/s00/s2");

		System.out.println("attr0 = " + attr0);
		System.out.println("attr1 = " + attr1);
		System.out.println("attr2 = " + attr2);

		n5.close();
	}
}
