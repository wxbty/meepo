
package org.feisoft.transaction.resource;

import org.feisoft.transaction.archive.XAResourceArchive;

import java.util.List;

public interface XATerminator extends javax.transaction.xa.XAResource {

	public List<XAResourceArchive> getResourceArchives();

}
