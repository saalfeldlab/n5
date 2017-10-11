/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.saalfeldlab.n5.s3;

import java.io.IOException;

import org.apache.commons.lang.RandomStringUtils;
import org.janelia.saalfeldlab.n5.N5TestBase;
import org.junit.BeforeClass;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import io.findify.s3mock.S3Mock;

/**
 * Initiates testing of Amazon Web Services S3-based N5 implementation using S3 mock library.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5S3MockTest extends N5TestBase {

	static private String testBucket = "test-bucket-n5-" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

	/**
	 * @throws IOException
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws IOException {

		final S3Mock api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
		api.start();

		final EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2");

		final AmazonS3 s3 = AmazonS3ClientBuilder
			      .standard()
			      .withPathStyleAccessEnabled(true)
			      .withEndpointConfiguration(endpoint)
			      .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
			      .build();

		n5 = new N5S3Writer(s3, testBucket);
		N5TestBase.setUpBeforeClass();
	}
}
