package org.janelia.saalfeldlab.n5;

import org.junit.Assert;

public class HierarchyStoreCounters {

	private int readAttrCallCount = 0;
	private int writeAttrCallCount = 0;
	private int removeAttrCallCount = 0;
	private int isDirCallCount = 0;
	private int rmDirCallCount = 0;
	private int mkDirCallCount = 0;
	private int listCallCount = 0;

	public static void assertEqualCounters(final HierarchyStoreCounters expected, final HierarchyStoreCounters actual) {
		Assert.assertEquals("readAttr", expected.readAttrCallCount, actual.readAttrCallCount);
		Assert.assertEquals("writeAttr", expected.writeAttrCallCount, actual.writeAttrCallCount);
		Assert.assertEquals("removeAttr", expected.removeAttrCallCount, actual.removeAttrCallCount);
		Assert.assertEquals("isDir", expected.isDirCallCount, actual.isDirCallCount);
		Assert.assertEquals("rmDir", expected.rmDirCallCount, actual.rmDirCallCount);
		Assert.assertEquals("mkDir", expected.mkDirCallCount, actual.mkDirCallCount);
		Assert.assertEquals("list", expected.listCallCount, actual.listCallCount);
	}

	public void reset() {
		readAttrCallCount = 0;
		writeAttrCallCount = 0;
		removeAttrCallCount = 0;
		isDirCallCount = 0;
		rmDirCallCount = 0;
		mkDirCallCount = 0;
		listCallCount = 0;
	}

	public void incReadAttr() {
		readAttrCallCount++;
	}

	public void incReadAttr(int increment) {
		readAttrCallCount += increment;
	}

	public void incWriteAttr() {
		writeAttrCallCount++;
	}

	public void incWriteAttr(int increment) {
		writeAttrCallCount += increment;
	}

	public void incRemoveAttr() {
		removeAttrCallCount++;
	}

	public void incRemoveAttr(int increment) {
		removeAttrCallCount += increment;
	}

	public void incIsDir() {
		isDirCallCount++;
	}

	public void incIsDir(int increment) {
		isDirCallCount += increment;
	}

	public void incRmDir() {
		rmDirCallCount++;
	}

	public void incRmDir(int increment) {
		rmDirCallCount += increment;
	}

	public void incMkDir() {
		mkDirCallCount++;
	}

	public void incMkDir(int increment) {
		mkDirCallCount += increment;
	}

	public void incList() {
		listCallCount++;
	}

	public void incList(int increment) {
		listCallCount += increment;
	}

	public int readAttrCallCount() {
		return readAttrCallCount;
	}

	public int writeAttrCallCount() {
		return writeAttrCallCount;
	}

	public int removeAttrCallCount() {
		return removeAttrCallCount;
	}

	public int isDirCallCount() {
		return isDirCallCount;
	}

	public int rmDirCallCount() {
		return rmDirCallCount;
	}

	public int mkDirCallCount() {
		return mkDirCallCount;
	}

	public int listCallCount() {
		return listCallCount;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("MetaStoreCounters{");
		sb.append("readAttr:").append(readAttrCallCount);
		sb.append(", writeAttr:").append(writeAttrCallCount);
		sb.append(", removeAttr:").append(removeAttrCallCount);
		sb.append(", isDir:").append(isDirCallCount);
		sb.append(", rmDir:").append(rmDirCallCount);
		sb.append(", mkDir:").append(mkDirCallCount);
		sb.append(", list:").append(listCallCount);
		sb.append('}');
		return sb.toString();
	}
}
