
/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.util;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

/**
 * Copy data set from one n5 root to another.
 *
 * @author Philipp Hanslovsky
 *
 *
 */
public class Copy
{

	/**
	 *
	 * {@Code Exception} to be thrown when a dataset cannot be found at a
	 * specific location
	 *
	 */
	@SuppressWarnings( "serial" )
	public static class DatasetDoesNotExistException extends Exception
	{
		public DatasetDoesNotExistException( final N5Reader n5, final String dataset )
		{
			super( String.format( "Datset %s not found in root %s", dataset, n5 ) );
		}

	}

	/**
	 * Create exact copy of entire N5
	 *
	 * @param source
	 *            N5 root to copy from
	 * @param target
	 *            N5 root to copy into
	 * @throws DatasetDoesNotExistException
	 *             Data set does not exist in {@code source}
	 * @throws IOException
	 */
	public static void copy(
			final N5Reader source,
			final N5Writer target ) throws DatasetDoesNotExistException, IOException
	{
		copy( source, target, "" );
	}

	/**
	 * Create exact copy of data set in different N5 root.
	 *
	 * @param source
	 *            N5 root to copy from
	 * @param target
	 *            N5 root to copy into
	 * @param from
	 *            Copy all sub-groups, starting at {@code from} (can be a data
	 *            set).
	 * @throws DatasetDoesNotExistException
	 *             Data set does not exist in {@code source}
	 * @throws IOException
	 */
	public static void copy(
			final N5Reader source,
			final N5Writer target,
			final String from ) throws DatasetDoesNotExistException, IOException
	{
		copy( source, target, from, unchecked( ( ds, attributes ) -> copyWithoutCreation( source, target, ds, attributes ) ) );
	}

	/**
	 * Create exact copy of entire N5
	 *
	 * @param source
	 *            N5 root to copy from
	 * @param target
	 *            N5 root to copy into
	 * @param es
	 * @param numTasks
	 * @return
	 * @throws DatasetDoesNotExistException
	 *             Data set does not exist in {@code source}
	 * @throws IOException
	 */
	public static List< Future< Void > > copy(
			final N5Reader source,
			final N5Writer target,
			final ExecutorService es,
			final int numTasks ) throws DatasetDoesNotExistException, IOException
	{
		return copy( source, target, "", es, numTasks );
	}

	/**
	 * Create exact copy of data set in different N5 root.
	 *
	 * @param source
	 *            N5 root to copy from
	 * @param target
	 *            N5 root to copy into
	 * @param from
	 *            Copy all sub-groups, starting at {@code from} (can be a data
	 *            set).
	 * @param es
	 * @param numTasks
	 * @throws DatasetDoesNotExistException
	 *             Data set does not exist in {@code source}
	 * @throws IOException
	 */
	public static List< Future< Void > > copy(
			final N5Reader source,
			final N5Writer target,
			final String from,
			final ExecutorService es,
			final int numTasks ) throws DatasetDoesNotExistException, IOException
	{
		final List< Future< Void > > futures = new ArrayList<>();

		final BiConsumer< String, DatasetAttributes > datasetCopyHandler = ( ds, attributes ) -> {
			final long numBlocks = numBlocks( attributes );
			final long step = Math.max( numBlocks / numTasks, 1 );
			for ( long start = 0; start < numBlocks; start += step )
			{
				final long fstart = start;
				futures.add( es.submit( asCallable( unchecked( () -> copyWithoutCreation( source, target, ds, attributes, fstart, Math.min( fstart + step, numBlocks ) ) ) ) ) );
			}
		};

		copy( source, target, from, datasetCopyHandler );

		return futures;
	}

	/**
	 * Create exact copy of data set in different N5 root. The caller provides a
	 * handler that copies individual data sets.
	 *
	 * @param source
	 *            N5 root to copy from
	 * @param target
	 *            N5 root to copy into
	 * @param from
	 *            Copy all sub-groups, starting at {@code from} (can be a data
	 *            set).
	 * @param handler
	 * @throws DatasetDoesNotExistException
	 *             Data set does not exist in {@code source}
	 * @throws IOException
	 */
	public static void copy(
			final N5Reader source,
			final N5Writer target,
			final String from,
			final BiConsumer< String, DatasetAttributes > datasetCopyHandler ) throws DatasetDoesNotExistException, IOException
	{
		final List< String > groups = listAll( source, from );

		for ( final String group : groups )
		{
			if ( source.datasetExists( group ) )
			{
				final DatasetAttributes attributes = source.getDatasetAttributes( group );
				target.createDataset( group, attributes );
				datasetCopyHandler.accept( group, attributes );
			}
			else
			{
				target.createGroup( group );
			}
			copyAttributes( source, target, group );
		}
	}

	/**
	 * Create exact copy of all blocks of data set from source N5 into target N5
	 * root. This does not create the data set in the target N5.
	 *
	 * @param source
	 *            N5 root to copy from
	 * @param target
	 *            N5 root to copy into
	 * @param dataset
	 *            Data set to be copied
	 * @param attributes
	 *            Data set attributes
	 * @throws DatasetDoesNotExistException
	 *             Data set does not exist in {@code source}
	 * @throws IOException
	 */
	public static void copyWithoutCreation(
			final N5Reader source,
			final N5Writer target,
			final String dataset,
			final DatasetAttributes attributes ) throws DatasetDoesNotExistException, IOException
	{
		copyWithoutCreation( source, target, dataset, attributes, 0, numBlocks( attributes ) );
	}

	/**
	 * Create exact copy of all blocks of data set within specified range in
	 * different N5 root. This does not create the data set in the target N5
	 * root. The range is defined by {@code startBlock} (inclusive) and
	 * {@code stopBlock} (exclusive):
	 * {@code range = [startBlock, stopBlock - 1]}
	 *
	 * @param source
	 *            N5 root to copy from
	 * @param target
	 *            N5 root to copy into
	 * @param dataset
	 *            Data set to be copied
	 * @param attributes
	 *            Data set attributes
	 * @param startBlock
	 * @param stopBlock
	 * @throws DatasetDoesNotExistException
	 *             Data set does not exist in {@code source}
	 * @throws IOException
	 */
	public static void copyWithoutCreation(
			final N5Reader source,
			final N5Writer target,
			final String dataset,
			final DatasetAttributes attributes,
			final long startBlock,
			final long stopBlock ) throws DatasetDoesNotExistException, IOException
	{
		final long[] gridDims = attributes.getDimensions().clone();
		divideInPlace( gridDims, attributes.getBlockSize() );
		final long[] blockPosition = new long[ gridDims.length ];
		for ( long block = startBlock; block < stopBlock; ++block )
		{
			indexToPosition( block, gridDims, blockPosition );
			final DataBlock< ? > data = source.readBlock( dataset, attributes, blockPosition );
			if ( data != null )
			{
				target.writeBlock( dataset, attributes, data );
			}
		}
	}

	/**
	 * Copy all attributes ignoring data set attributes if group is a data set.
	 *
	 * @param source
	 * @param target
	 * @param groupName
	 * @throws IOException
	 */
	public static void copyAttributes( final N5Reader source, final N5Writer target, final String groupName )
			throws IOException
	{

		final Map< String, Class< ? > > attributes = source.listAttributes( groupName );

		final DatasetAttributes datasetAttributes = source.datasetExists( groupName ) ? source.getDatasetAttributes( groupName ) : null;
		final Set< String > datasetAttributeKeys = datasetAttributes == null ? new HashSet<>() : datasetAttributes.asMap().keySet();
		attributes
		.entrySet()
		.stream()
		.filter( e -> !datasetAttributeKeys.contains( e.getKey() ) )
		.forEach( unchecked( e -> target.setAttribute( groupName, e.getKey(), source.getAttribute( groupName, e.getKey(), e.getValue() ) ) ) );
	}

	/**
	 * list all contained groups/data sets
	 *
	 * @param n5
	 * @return
	 * @throws IOException
	 */
	public static List< String > listAll( final N5Reader n5 ) throws IOException
	{
		return listAll( n5, "" );
	}

	/**
	 * list all contained groups/data sets starting at {@code from}
	 *
	 * @param n5
	 * @param from
	 * @return
	 * @throws IOException
	 */
	public static List< String > listAll( final N5Reader n5, final String from ) throws IOException
	{
		final List< String > store = new ArrayList<>();
		listAll( n5, from, store );
		return store;
	}

	private static void listAll( final N5Reader n5, final String from, final List< String > store ) throws IOException
	{
		store.add( from );
		if ( n5.datasetExists( from ) ) { return; }

		final String[] subGroups = n5.list( from );
		for ( final String g : subGroups )
		{
			listAll( n5, Paths.get( from, g ).toString(), store );
		}
	}

	/**
	 * Determine number of blocks within data set
	 *
	 * @param attributes
	 * @return
	 */
	public static long numBlocks( final DatasetAttributes attributes )
	{
		return product( divideInPlace( attributes.getDimensions().clone(), attributes.getBlockSize() ) );
	}

	/**
	 *
	 * {@link Consumer} equivalent that throws checked {@link Exception}.
	 *
	 * @param <T>
	 * @param <U>
	 */
	public static interface CheckedConsumer< T >
	{
		public void accept( T t ) throws Exception;
	}

	/**
	 *
	 * Use {@link CheckedConsumer} as {@link Consumer} by converting any checked
	 * {@link Exception} into {@link RuntimeException}.
	 *
	 * @param consumer
	 * @return
	 */
	public static < T > Consumer< T > unchecked( final CheckedConsumer< T > consumer )
	{
		return t -> {
			try
			{
				consumer.accept( t );
			}
			catch ( final Exception e )
			{
				throw e instanceof RuntimeException ? ( RuntimeException ) e : new RuntimeException( e );
			}
		};
	}

	/**
	 *
	 * {@link BiConsumer} equivalent that throws checked {@link Exception}.
	 *
	 * @param <T>
	 * @param <U>
	 */
	public static interface CheckedBiConsumer< T, U >
	{
		public void accept( T t, U u ) throws Exception;
	}

	/**
	 *
	 * Use {@link CheckedBiConsumer} as {@link Consumer} by converting any
	 * checked {@link Exception} into {@link RuntimeException}.
	 *
	 * @param consumer
	 * @return
	 */
	public static < T, U > BiConsumer< T, U > unchecked( final CheckedBiConsumer< T, U > consumer )
	{
		return ( t, u ) -> {
			try
			{
				consumer.accept( t, u );
			}
			catch ( final Exception e )
			{
				throw e instanceof RuntimeException ? ( RuntimeException ) e : new RuntimeException( e );
			}
		};
	}

	/**
	 *
	 * {@link Runnable} equivalent that throws checked {@link Exception}.
	 *
	 */
	public static interface CheckedRunnable
	{
		public void run() throws Exception;
	}

	/**
	 *
	 * Use {@link CheckedRunnable} as {@link Runnable} by converting any checked
	 * {@link Exception} into {@link RuntimeException}.
	 *
	 * @param consumer
	 * @return
	 */
	public static Runnable unchecked( final CheckedRunnable r )
	{
		return () -> {
			try
			{
				r.run();
			}
			catch ( final Exception e )
			{
				throw e instanceof RuntimeException ? ( RuntimeException ) e : new RuntimeException( e );
			}
		};
	}

	/**
	 *
	 * convenience method to use a {@link Runnable} as {@link Callable< Void >}.
	 *
	 * @param r
	 * @return
	 */
	public static Callable< Void > asCallable( final Runnable r )
	{
		return () -> {
			r.run();
			return null;
		};
	}

	public static long[] divideInPlace( final long[] dimensions, final int[] blockSize )
	{
		Arrays.setAll( dimensions, d -> ( long ) Math.ceil( dimensions[ d ] * 1.0 / blockSize[ d ] ) );
		return dimensions;
	}

	public static long product( final long[] array )
	{
		return Arrays.stream( array ).reduce( 1, ( l1, l2 ) -> l1 * l2 );
	}

	final static public void indexToPosition( long index, final long[] dimensions, final long[] position )
	{
		final int maxDim = dimensions.length - 1;
		for ( int d = 0; d < maxDim; ++d )
		{
			final long j = index / dimensions[ d ];
			position[ d ] = index - j * dimensions[ d ];
			index = j;
		}
		position[ maxDim ] = index;
	}

}
