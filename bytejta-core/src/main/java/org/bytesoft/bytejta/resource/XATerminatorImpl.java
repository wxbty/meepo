/**
 * Copyright 2014-2016 yangming.liu<bytefox@126.com>.
 * <p>
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, see <http://www.gnu.org/licenses/>.
 */
package org.bytesoft.bytejta.resource;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.bytesoft.bytejta.image.BackInfo;
import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.resource.XATerminator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XATerminatorImpl implements XATerminator {
    static final Logger logger = LoggerFactory.getLogger(XATerminatorImpl.class);

    private TransactionBeanFactory beanFactory;
    private final List<XAResourceArchive> resources = new ArrayList<XAResourceArchive>();

    public static HashMap<String, String> sourceProp = new HashMap<String, String>();

    public synchronized int prepare(Xid xid) throws XAException {
        TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

        int globalVote = XAResource.XA_RDONLY;
        for (int i = 0; i < this.resources.size(); i++) {
            XAResourceArchive archive = this.resources.get(i);

            boolean prepared = archive.getVote() != XAResourceArchive.DEFAULT_VOTE;
            if (prepared) {
                globalVote = archive.getVote() == XAResource.XA_RDONLY ? globalVote : XAResource.XA_OK;
            } else {
                int branchVote = archive.prepare(archive.getXid());
                archive.setVote(branchVote);
                if (branchVote == XAResource.XA_RDONLY) {
                    archive.setReadonly(true);
                    archive.setCompleted(true);
                } else {
                    globalVote = XAResource.XA_OK;
                }
                transactionLogger.updateResource(archive);

            }

            logger.info("[{}] prepare: xares= {}, branch= {}, vote= {}",
                    ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
                    ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), archive.getVote());
        }

        return globalVote;
    }


    /**
     * error: XA_HEURHAZ, XA_HEURMIX, XA_HEURCOM, XA_HEURRB, XA_RDONLY, XAER_RMERR
     */
    public synchronized void commit(Xid xid, boolean onePhase) throws XAException {
        if (onePhase) {
            this.fireOnePhaseCommit(xid);
        } else {
            this.fireTwoPhaseCommit(xid);
        }
    }

    private void fireOnePhaseCommit(Xid xid) throws XAException {

        if (this.resources.size() == 0) {
            throw new XAException(XAException.XA_RDONLY);
        } else if (this.resources.size() > 1) {
            this.rollback(xid);
            throw new XAException(XAException.XA_HEURRB);
        }

        TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

        XAResourceArchive archive = this.resources.get(0);

        if (archive.isCommitted() && archive.isRolledback()) {
            throw new XAException(XAException.XA_HEURMIX);
        } else if (archive.isCommitted()) {
            return;
        } else if (archive.isReadonly()) {
            throw new XAException(XAException.XA_RDONLY);
        } else if (archive.isRolledback()) {
            throw new XAException(XAException.XA_HEURRB);
        }

        boolean updateRequired = true;
        try {
            this.invokeOnePhaseCommit(archive);

            archive.setCommitted(true);
            archive.setCompleted(true);

            logger.info("[{}] commit: xares= {}, branch= {}, opc= {}",
                    ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
                    ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), true);
        } catch (XAException xaex) {
            logger.error("[{}] Error occurred while committing xa-resource: xares= {}, branch= {}, code= {}",
                    ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
                    ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), xaex.errorCode, xaex);

            switch (xaex.errorCode) {
                case XAException.XA_HEURCOM:
                    archive.setHeuristic(true);
                    archive.setCommitted(true);
                    archive.setCompleted(true);
                    break;
                case XAException.XA_HEURHAZ:
                    archive.setHeuristic(true);
                    throw xaex;
                case XAException.XA_HEURMIX:
                    archive.setHeuristic(true);
                    archive.setCommitted(true);
                    archive.setRolledback(true);
                    archive.setCompleted(true);
                    throw xaex;
                case XAException.XA_HEURRB:
                    archive.setHeuristic(true);
                    archive.setRolledback(true);
                    archive.setCompleted(true);
                    throw xaex;
                case XAException.XAER_RMFAIL:
                    updateRequired = false;
                    throw new XAException(XAException.XA_HEURHAZ);
                case XAException.XAER_RMERR:
                default:
                    updateRequired = false;
                    throw new XAException(XAException.XAER_RMERR);
            }
        } catch (RuntimeException rex) {
            logger.error("[{}] Error occurred while committing xa-resource: xares= {}, branch= {}",
                    ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
                    ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), rex);
            updateRequired = false;
            throw new XAException(XAException.XA_HEURHAZ);
        } finally {
            if (updateRequired) {
                transactionLogger.updateResource(archive);
            }
        }
    }

    private void fireTwoPhaseCommit(Xid xid) throws XAException {
        TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

        boolean committedExists = false;
        boolean rolledbackExists = false;
        boolean unFinishExists = false;
        boolean errorExists = false;

        for (int i = this.resources.size() - 1; i >= 0; i--) {
            XAResourceArchive archive = this.resources.get(i);

            if (archive.isCommitted() && archive.isRolledback()) {
                committedExists = true;
                rolledbackExists = true;
                continue;
            } else if (archive.isCommitted()) {
                committedExists = true;
                continue;
            } else if (archive.isReadonly()) {
                continue;
            } else if (archive.isRolledback()) {
                rolledbackExists = true;
                continue;
            }

            Xid branchXid = archive.getXid();
            boolean updateRequired = true;
            try {
                this.invokeTwoPhaseCommit(archive);
                committedExists = true;
                archive.setCommitted(true);
                archive.setCompleted(true);
                logger.info("[{}] commit: xares= {}, branch= {}, onePhaseCommit= {}",
                        ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), archive,
                        ByteUtils.byteArrayToString(branchXid.getBranchQualifier()), false);
            } catch (XAException xaex) {
                logger.error("[{}] Error occurred while committing xa-resource: xares= {}, branch= {}, code= {}",
                        ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
                        ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), xaex.errorCode, xaex);

                switch (xaex.errorCode) {
                    case XAException.XA_HEURHAZ:
                        archive.setHeuristic(true);
                        unFinishExists = true;
                        break;
                    case XAException.XA_HEURMIX:
                        committedExists = true;
                        rolledbackExists = true;

                        archive.setCommitted(true);
                        archive.setRolledback(true);
                        archive.setHeuristic(true);
                        archive.setCompleted(true);
                        break;
                    case XAException.XA_HEURCOM:
                        committedExists = true;
                        archive.setCommitted(true);
                        archive.setHeuristic(true);
                        archive.setCompleted(true);
                        break;
                    case XAException.XA_HEURRB:
                        rolledbackExists = true;
                        archive.setRolledback(true);
                        archive.setHeuristic(true);
                        archive.setCompleted(true);
                        break;
                    case XAException.XAER_RMFAIL:
                        unFinishExists = true;
                        updateRequired = false;
                        break;
                    case XAException.XA_RDONLY:
                        archive.setReadonly(true);
                        break;
                    case XAException.XAER_RMERR:
                    default:
                        errorExists = true;
                        updateRequired = false;
                }
            } catch (RuntimeException rex) {
                logger.error("[{}] Error occurred while committing xa-resource: xares= {}, branch= {}",
                        ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
                        ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), rex);
                unFinishExists = true;
                updateRequired = false;
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (updateRequired) {
                    transactionLogger.updateResource(archive);
                }
            }

        } // end-for

        if (committedExists && rolledbackExists) {
            throw new XAException(XAException.XA_HEURMIX);
        } else if (unFinishExists) {
            throw new XAException(XAException.XA_HEURHAZ);
        } else if (errorExists) {
            throw new XAException(XAException.XAER_RMERR);
        } else if (rolledbackExists) {
            throw new XAException(XAException.XA_HEURRB);
        } else if (committedExists == false) {
            throw new XAException(XAException.XA_RDONLY);
        }

    }

    private void invokeOnePhaseCommit(XAResourceArchive archive) throws XAException {
        try {
            archive.commit(archive.getXid(), true);
        } catch (XAException xaex) {
            switch (xaex.errorCode) {
                case XAException.XA_HEURCOM:
                case XAException.XA_HEURHAZ:
                case XAException.XA_HEURMIX:
                case XAException.XA_HEURRB:
                    logger.warn("An error occurred in one phase commit: {}, transaction has been completed!",
                            ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()));
                    throw xaex;
                case XAException.XAER_RMFAIL:
                    logger.warn("An error occurred in one phase commit: {}",
                            ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()));
                    throw xaex;
                case XAException.XAER_NOTA:
                case XAException.XAER_INVAL:
                case XAException.XAER_PROTO:
                    logger.warn("An error occurred in one phase commit: {}",
                            ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()));
                    throw new XAException(XAException.XAER_RMERR);
                case XAException.XAER_RMERR:
                case XAException.XA_RBCOMMFAIL:
                case XAException.XA_RBDEADLOCK:
                case XAException.XA_RBINTEGRITY:
                case XAException.XA_RBOTHER:
                case XAException.XA_RBPROTO:
                case XAException.XA_RBROLLBACK:
                case XAException.XA_RBTIMEOUT:
                case XAException.XA_RBTRANSIENT:
                default:
                    logger.warn("An error occurred in one phase commit: {}, transaction has been rolled back!",
                            ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()));
                    throw new XAException(XAException.XA_HEURRB);
            }
        }
    }

    private void invokeTwoPhaseCommit(XAResourceArchive archive) throws XAException, SQLException {
        //清理锁信息来代替数据库提交
//            archive.commit(archive.getXid(), false);
        if (archive.getDescriptor() instanceof RemoteResourceDescriptor) {
            archive.commit(archive.getXid(), false);
            return;
        } else {
            archive.releaseLock();
        }
    }

    /**
     * error: XA_HEURHAZ, XA_HEURMIX, XA_HEURCOM, XA_HEURRB, XA_RDONLY, XAER_RMERR
     */
    public synchronized void rollback(Xid xid) throws XAException {
        TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

        boolean committedExists = false;
        boolean rolledbackExists = false;
        boolean unFinishExists = false;
        boolean errorExists = false;
        for (int i = 0; i < this.resources.size(); i++) {
            XAResourceArchive archive = this.resources.get(i);

            if (archive.isCommitted() && archive.isRolledback()) {
                committedExists = true;
                rolledbackExists = true;
                continue;
            } else if (archive.isRolledback()) {
                rolledbackExists = true;
                continue;
            } else if (archive.isReadonly()) {
                continue;
            } else if (archive.isCommitted()) {
                committedExists = true;
                continue;
            }

            boolean updateRequired = true;
            try {
                this.invokeRollback(archive);
                rolledbackExists = true;
                archive.setRolledback(true);
                archive.setCompleted(true);
                logger.info("[{}] rollback: xares= {}, branch= {}",
                        ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
                        ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()));
            } catch (XAException xaex) {
                logger.error("[{}] Error occurred while rolling back xa-resource: xares= {}, branch= {}, code= {}",
                        ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
                        ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), xaex.errorCode, xaex);

                switch (xaex.errorCode) {
                    case XAException.XA_HEURHAZ:
                        unFinishExists = true;
                        archive.setHeuristic(true);
                        break;
                    case XAException.XA_HEURMIX:
                        committedExists = true;
                        rolledbackExists = true;
                        archive.setCommitted(true);
                        archive.setRolledback(true);
                        archive.setHeuristic(true);
                        archive.setCompleted(true);
                        break;
                    case XAException.XA_HEURCOM:
                        committedExists = true;
                        archive.setCommitted(true);
                        archive.setHeuristic(true);
                        archive.setCompleted(true);
                        break;
                    case XAException.XA_HEURRB:
                        rolledbackExists = true;
                        archive.setRolledback(true);
                        archive.setHeuristic(true);
                        archive.setCompleted(true);
                        break;
                    case XAException.XA_RDONLY:
                        archive.setReadonly(true);
                        archive.setCompleted(true);
                        break;
                    case XAException.XAER_RMFAIL:
                        unFinishExists = true;
                        updateRequired = false;
                        break;
                    case XAException.XAER_RMERR:
                    default:
                        errorExists = true;
                        updateRequired = false;
                }
            } catch (RuntimeException rex) {
                unFinishExists = true;
                updateRequired = false;
                logger.error("[{}] Error occurred while rolling back xa-resource: xares= {}, branch= {}",
                        ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
                        ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), rex);
            } finally {
                if (updateRequired) {
                    transactionLogger.updateResource(archive);
                }
            }
        }

        if (committedExists && rolledbackExists) {
            throw new XAException(XAException.XA_HEURMIX);
        } else if (unFinishExists) {
            throw new XAException(XAException.XA_HEURHAZ);
        } else if (errorExists) {
            throw new XAException(XAException.XAER_RMERR);
        } else if (committedExists) {
            throw new XAException(XAException.XA_HEURCOM);
        } else if (rolledbackExists == false) {
            throw new XAException(XAException.XA_RDONLY);
        }

    }

    private void invokeRollback(XAResourceArchive archive) throws XAException {
        //-1已提交状态，应用级回滚，否则数据库级回滚
        if (archive.getVote() == -1) {

            if (archive.getDescriptor() instanceof RemoteResourceDescriptor) {
                archive.rollback(archive.getXid());
                return;
            }
            logger.info("XATerminatorImpl.bengin invokeRollback Of" + archive.getDescriptor().getDelegate().getClass().getName());
            XAConnection connection = (XAConnection) archive.getDescriptor().getDelegate();
            PreparedStatement ps = null;
            //拼接PREPARE语句，在general_log查找执行中的sql
            StringBuilder commandBuf = new StringBuilder(300);
            String GloableXid = archive.partGloableXid(archive.getXid());
            String branchXid = archive.partBranchXid(archive.getXid());
            String sqlStr = "select id, rollback_info from txc_undo_log where branch_id ='" + branchXid + "' and xid ='" + GloableXid + "'";
            Statement stmt = null;
            ResultSet rs = null;
            List<Long> ids = new ArrayList<>();
            List<String> backInfo = null;
            Connection conn = null;
            Map<Long, String> map = new HashMap<>();
            try {
                conn = connection.getConnection();
                stmt = conn.createStatement();
                rs = stmt.executeQuery(sqlStr);
                while (rs.next()) {

                    map.put(rs.getLong("id"), rs.getString("rollback_info"));
                }
                if (map.size() > 0) {
                    logger.info("bengin  invokeRollback rollbackinfo=" + map.keySet());
                    if (!rollback(map, conn, stmt)) {
                        logger.error(String.format("Roll back mysql info error!,backInfo:" + backInfo.toArray().toString()));
                    }
                }
                archive.releaseLock();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                //关闭自创建的连接
                try {
                    rs.close();
                    conn.close();
                    stmt.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        } else {
            logger.error("XAResourceArchive.ErrorVoteNum,vote =" + archive.getVote());
            throw new XAException("XAResourceArchive.ErrorVoteNum");
        }


    }

    public int getTransactionTimeout() throws XAException {
        throw new XAException(XAException.XAER_RMFAIL);
    }

    public boolean setTransactionTimeout(int seconds) throws XAException {
        throw new XAException(XAException.XAER_RMFAIL);
    }

    public void start(Xid xid, int flags) throws XAException {
        TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

        int globalVote = XAResource.XA_RDONLY;
        for (int i = 0; i < this.resources.size(); i++) {
            XAResourceArchive archive = this.resources.get(i);
            logger.info("[{}] prepare: xares= {}, branch= {}, vote= {}",
                    ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
                    ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), archive.getVote());
        }

        return;
    }


    public void end(Xid xid, int flags) throws XAException {
        throw new XAException(XAException.XAER_RMFAIL);
    }

    public boolean isSameRM(XAResource xares) throws XAException {
        throw new XAException(XAException.XAER_RMFAIL);
    }

    public Xid[] recover(int flag) throws XAException {
        throw new XAException(XAException.XAER_RMFAIL);
    }

    public void forget(Xid xid) throws XAException {
        for (int i = 0; i < this.resources.size(); i++) {
            XAResourceArchive archive = this.resources.get(i);
            Xid currentXid = archive.getXid();
            if (archive.isHeuristic() == false) {
                continue;
            }

            try {
                Xid branchXid = archive.getXid();
                archive.forget(branchXid);
            } catch (XAException xae) {
                // Possible exception values are XAER_RMERR, XAER_RMFAIL
                // , XAER_NOTA, XAER_INVAL, or XAER_PROTO.
                switch (xae.errorCode) {
                    case XAException.XAER_RMERR:
                        logger.error("[{}] forget: xares= {}, branch={}, error= {}",
                                ByteUtils.byteArrayToString(currentXid.getGlobalTransactionId()), archive,
                                ByteUtils.byteArrayToString(currentXid.getBranchQualifier()), xae.errorCode);
                        break;
                    case XAException.XAER_RMFAIL:
                        logger.error("[{}] forget: xares= {}, branch={}, error= {}",
                                ByteUtils.byteArrayToString(currentXid.getGlobalTransactionId()), archive,
                                ByteUtils.byteArrayToString(currentXid.getBranchQualifier()), xae.errorCode);
                        break;
                    case XAException.XAER_NOTA:
                    case XAException.XAER_INVAL:
                    case XAException.XAER_PROTO:
                        break;
                    default:
                        logger.error("[{}] forget: xares= {}, branch={}, error= {}",
                                ByteUtils.byteArrayToString(currentXid.getGlobalTransactionId()), archive,
                                ByteUtils.byteArrayToString(currentXid.getBranchQualifier()), xae.errorCode);
                }
            }

        } // end-for
    }

    public List<XAResourceArchive> getResourceArchives() {
        return this.resources;
    }

    public TransactionBeanFactory getBeanFactory() {
        return beanFactory;
    }

    public void setBeanFactory(TransactionBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }


    private boolean rollback(Map<Long, String> map, Connection connection, Statement stmt) throws XAException, SQLException {

        for (Long id : map.keySet()) {
            String imageInfo = map.get(id);
            System.out.println("XATerminatorImpl.ExeBackinfo:" + imageInfo);

            BackInfo backInfo = JSON.parseObject(imageInfo, new TypeReference<BackInfo>() {
            });

            backInfo.rollback(stmt);
            backInfo.setId(id);
            backInfo.updateStatusFinish(stmt);
        }
        return true;
    }


}
