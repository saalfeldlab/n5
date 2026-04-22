package org.janelia.saalfeldlab.n5.locking;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JustFileChannelsThreaded {

	public static void main(String[] args) throws InterruptedException {

		final int nThreads = Integer.parseInt(args[0]);
		final String[] subArgs = new String[args.length - 1];
		System.arraycopy(args, 1, subArgs, 0, args.length - 1);

		final ExecutorService exec = Executors.newFixedThreadPool(nThreads);
		for( int i = 0; i < nThreads; i++ ) {
			exec.submit( () -> {
				try {
					Thread.sleep(200);
					JustFileChannels.main(subArgs);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}
		exec.awaitTermination(5, TimeUnit.MINUTES);
	}
}