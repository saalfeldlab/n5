package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.N5Exception;

public class ShardException extends N5Exception {

	private static final long serialVersionUID = -77907634621557855L;

	public static class IndexException extends ShardException {

		private static final long serialVersionUID = 3924426352575114063L;

	}
}
