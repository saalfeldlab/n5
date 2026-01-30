package org.janelia.saalfeldlab.n5;

import java.io.PrintStream;

public class DebugHelpers
{
	public static void printStackTrace( String ... unlessContains )
	{
		printStackTrace( System.out, 3, -1, unlessContains );
	}

	public static void printStackTrace( int maxDepth, String ... unlessContains )
	{
		printStackTrace( System.out, 3, maxDepth, unlessContains );
	}

	public static void printStackTrace( PrintStream out, int maxDepth, String ... unlessContains )
	{
		printStackTrace( out, 3, maxDepth, unlessContains );
	}

	public static void printStackTrace( PrintStream out, int startDepth, int maxDepth, String ... unlessContains )
	{
		final StackTraceElement[] trace = Thread.currentThread().getStackTrace();

		for ( StackTraceElement element : trace )
		{
			final String traceLine = element.toString();
			for ( String template : unlessContains )
				if ( traceLine.contains( template ) )
					return;
		}

		final int len = ( maxDepth < 0 )
				? trace.length
				: Math.min( startDepth + maxDepth, trace.length );
		for ( int i = startDepth; i < len; ++i )
		{
			final String prefix = ( i == startDepth ) ? "" : "    at ";
			out.println( prefix + trace[ i ].toString() );
		}

		out.println();
	}
}
