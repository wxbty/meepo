/**
 * Copyright 2014-2016 yangming.liu<bytefox@126.com>.
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, see <http://www.gnu.org/licenses/>.
 */

package org.feisoft.transaction.archive;

import org.feisoft.common.utils.DbPool.DbPoolSource;
import org.feisoft.common.utils.SpringBeanUtil;
import org.feisoft.jta.supports.resource.RemoteResourceDescriptor;
import org.feisoft.transaction.supports.resource.XAResourceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.SQLException;

public class XAResourceArchive implements XAResource {

    private DbPoolSource dbPoolSource = null;

    {
        if (this.dbPoolSource == null) {
            DbPoolSource dbPoolSource = (DbPoolSource) SpringBeanUtil.getBean("dbPoolSource");
            this.dbPoolSource = dbPoolSource;
        }
    }

    static final Logger logger = LoggerFactory.getLogger(XAResourceArchive.class);

    public static final int DEFAULT_VOTE = -1;

    private boolean suspended;

    private boolean delisted;

    private boolean completed;

    private boolean readonly;

    private boolean committed;

    private boolean rolledback;

    private boolean heuristic;

    private boolean identified;

    private transient boolean recovered;

    private Xid xid;

    private int vote = DEFAULT_VOTE;

    private XAResourceDescriptor descriptor;

    public void commit(Xid ignore, boolean onePhase) throws XAException {
        if (this.readonly) {
            // ignore
        } else if (this.committed) {
            // ignore
        } else if (this.rolledback) {
            throw new XAException(XAException.XA_HEURRB);
        } else {

            if (getDescriptor() instanceof RemoteResourceDescriptor) {
                getDescriptor().commit(getXid(), false);
                return;
            } else {
                try {
                    releaseLock();
                } catch (SQLException e) {
                    logger.error("release sql error", e);
                    throw new XAException("release sql error");
                }
            }

            //			descriptor.commit(xid, onePhase);
        }
    }

    public void end(Xid ignore, int flags) throws XAException {
        descriptor.end(xid, flags);
    }

    public void forget(Xid ignore) throws XAException {
        descriptor.forget(xid);
    }

    public void forgetQuietly(Xid ignore) {
        try {
            descriptor.forget(xid);
        } catch (XAException ex) {
            logger.warn("Error occurred while forgeting xa-resource.", xid);
        }
    }

    public int getTransactionTimeout() throws XAException {
        return descriptor.getTransactionTimeout();
    }

    public boolean isSameRM(XAResource xares) throws XAException {
        if (XAResourceArchive.class.isInstance(xares)) {
            XAResourceArchive archive = (XAResourceArchive) xares;
            return descriptor.isSameRM(archive.getDescriptor());
        } else {
            return descriptor.isSameRM(xares);
        }
    }

    public int prepare(Xid ignore) throws XAException {
        if (this.vote == -1) {
            this.vote = this.descriptor.prepare(this.xid);
            this.readonly = this.vote == XAResource.XA_RDONLY;
        }
        return this.vote;

    }

    public Xid[] recover(int flag) throws XAException {
        return descriptor.recover(flag);
    }

    public void rollback(Xid ignore) throws XAException {

        if (this.readonly) {
            // ignore
        } else if (this.committed) {
            throw new XAException(XAException.XA_HEURCOM);
        } else if (this.rolledback) {
            // ignore
        } else {
            descriptor.rollback(xid);
        }

    }

    public boolean setTransactionTimeout(int seconds) throws XAException {
        return descriptor.setTransactionTimeout(seconds);
    }

    public void start(Xid ignore, int flags) throws XAException {
        descriptor.start(xid, flags);
    }

    public String toString() {
        return String.format("xa-res-archive[descriptor: %s]", this.descriptor);
    }

    public XAResourceDescriptor getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(XAResourceDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    public boolean isSuspended() {
        return suspended;
    }

    public void setSuspended(boolean suspended) {
        this.suspended = suspended;
    }

    public boolean isDelisted() {
        return delisted;
    }

    public void setDelisted(boolean delisted) {
        this.delisted = delisted;
    }

    public Xid getXid() {
        return xid;
    }

    public void setXid(Xid xid) {
        this.xid = xid;
    }

    public int getVote() {
        return vote;
    }

    public void setVote(int vote) {
        this.vote = vote;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    public boolean isRolledback() {
        return rolledback;
    }

    public void setRolledback(boolean rolledback) {
        this.rolledback = rolledback;
    }

    public boolean isReadonly() {
        return readonly;
    }

    public void setReadonly(boolean readonly) {
        this.readonly = readonly;
    }

    public boolean isHeuristic() {
        return heuristic;
    }

    public void setHeuristic(boolean heuristic) {
        this.heuristic = heuristic;
    }

    public boolean isRecovered() {
        return recovered;
    }

    public void setRecovered(boolean recovered) {
        this.recovered = recovered;
    }

    public boolean isIdentified() {
        return identified;
    }

    public void setIdentified(boolean identified) {
        this.identified = identified;
    }

    public void releaseLock() throws SQLException {
        String gloableXid = partGloableXid(getXid());
        String branchXid = partBranchXid(getXid());
        String sql = "delete from txc_lock where xid ='" + gloableXid + "' and branch_id='" + branchXid + "' ";
        dbPoolSource.executeUpdate(sql);
        try {
            sql = "delete from txc_undo_log where xid ='" + gloableXid + "' and branch_id='" + branchXid + "' ";
            logger.info("releaseLock.releaseLog = {},currentTime={}", sql, System.currentTimeMillis());
            dbPoolSource.executeUpdate(sql);
        } catch (SQLException e) {
            logger.error("e",e);
        }
    }

    public String partGloableXid(Xid xid) {

        byte[] gtrid = xid.getGlobalTransactionId();

        StringBuilder builder = new StringBuilder();

        if (gtrid != null) {
            appendAsHex(builder, gtrid);
        }

        return builder.toString();
    }

    public String partBranchXid(Xid xid) {

        byte[] btrid = xid.getBranchQualifier();

        StringBuilder builder = new StringBuilder();

        if (btrid != null) {
            appendAsHex(builder, btrid);
        }

        return builder.toString();
    }

    private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f' };

    public static void appendAsHex(StringBuilder builder, byte[] bytes) {
        builder.append("0x");
        for (byte b : bytes) {
            builder.append(HEX_DIGITS[(b >>> 4) & 0xF]).append(HEX_DIGITS[b & 0xF]);
        }
    }

}
