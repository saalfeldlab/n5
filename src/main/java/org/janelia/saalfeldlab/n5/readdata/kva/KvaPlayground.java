package org.janelia.saalfeldlab.n5.readdata.kva;

public class KvaPlayground {


	// Playground for prototyping the ReadData that we get from a KeyValueAccess.
	//
	// This includes the following features:
	// (*) LazyRead
	//     This interface will be extended to be Closable.
	//     The FileSystem implementation creates a LockedChannel on construction and releases it on close()
	//     Actual read requests create their own channels.
	//     New read requests should fail when the LazyRead is already closed.
	// (*) KvaReadData
	//     Implemented via LazyRead.
	// (*) ClosableReadData
	//     A ReadData that implements Closable.
	//     The close() method simply calls a Runnable provided on construction.
	//     All ReadData methods are forwarded to a delegate. In particular,
	//     slices will not be Closable.
	// (*) LazySlicingReadData
	//     A ReadData whose slice methods produce also LazySlicingReadData.
	//     Only when materializing methods are called, a slice is obtained from
	//     the delegate and the operation carried out on that.
	// (*) SliceTrackingReadData
	//     When a slice is taken, the returned slice is already materialized.
	//     Keeps an internal cache of (materialized) slices and first tries to
	//     satisfy slice requests from this cache. If a slice request cannot be obtained from the cache,
	//     the slice is taken from the delegate and materialized.
	//     This is supposed to be wrapped with LazySlicingReadData.
	//     The combination of LazySlicingReadData and SliceTrackingReadData
	//     allows to take slices (and slices of slices) that are only
	//     materialized when necessary, but if materialized are cached correctly.
	//
	// Clever handling of ReadData::inputStream is postponed until later --
	// LazySlicingReadData will materialize when an inputStream is requested.
	//
	// The ReadData with all features that KeyValueAccess can provide is:
	//   LazyRead -> KvaReadData -> SliceTrackingReadData -> LazySlicingReadData -> ClosableReadData
	// FileKeyValueAccess should probably only do this:
	//   LazyRead -> KvaReadData -> ClosableReadData
	// (But for testing we can put everything there as well.)
	//
	// TODO:
	//   [+] Let LazyRead extends Closable
	//   [+] Implement KvaReadData (mostly copy from KeyValueAccessReadData) --> LazyReadData
	//   [+] Implement ClosableReadData


	// Revised plan:
	// KvaReadData/KeyValueAccessReadData was renamed to LazyReadData.
	// It behaves exactly as the LazySlicingReadData above, so we don't need that.
	// SliceTrackingReadData should become SliceTrackingLazyRead.
	// The chain then looks like this:
	//   LazyRead -> SliceTrackingLazyRead -> LazyReadData -> ClosableReadData
	// and for FileKeyValueAccess:
	//   LazyRead -> LazyReadData -> ClosableReadData
	// (But for testing we can put everything there as well.)

	// TODO:
	//   [+] Move LazyRead out of KeyValueAccess
	//   [+] Revise SliceTrackingReadData to be a LazyRead instead
	//   [ ] Implement the following behaviour for FileLazyRead:
	//         Create a LockedChannel on construction and releases it on close()
	//         Actual read requests create their own channels.
	//         New read requests should fail when the LazyRead is already closed.
	//   [ ] DefaultSegmentedReadData should extend DelegatingReadData (if we add that as a general utility)
	//   [ ] propagate prefetch() through delegates and eventually to the LazyRead. (Also, add prefetch() to LazyRead interface).
}
