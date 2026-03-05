package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;
import java.net.URI;
import java.net.URISyntaxException;

public class RootedKvaPlayground {

	public static void main(String[] args) throws URISyntaxException {

		final String basePath = "/Users/pietzsch/workspace/data/111010_weber_full.n5";
		final GsonBuilder gsonBuilder = new GsonBuilder();
		final boolean cacheMeta = false;


		final URI uri0 = new URI("t00001/s00/s0/attributes.json");
		final URI uri1 = new URI("t00001/s00/s1////attributes.json");
		System.out.println("uri0 = " + uri0.normalize());
		System.out.println("uri1 = " + uri1.normalize());

		final N5Reader n5 =  new N5FSReader(basePath, gsonBuilder, cacheMeta);

		final DatasetAttributes attr0 = n5.getDatasetAttributes("t00001/s00/s0");
		final DatasetAttributes attr1 = n5.getDatasetAttributes("t00001/s00/s1");
		final DatasetAttributes attr2 = n5.getDatasetAttributes("t00001/s00/s2");

		System.out.println("attr0 = " + attr0);
		System.out.println("attr1 = " + attr1);
		System.out.println("attr2 = " + attr2);

		n5.close();

//		System.out.println("new URI(\"\").resolve(new URI(\"attributes.json\")) = " + new URI("group/").resolve(new URI("attributes.json")));
	}
}
