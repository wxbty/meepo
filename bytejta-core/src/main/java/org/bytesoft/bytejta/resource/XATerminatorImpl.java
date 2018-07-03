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
import com.alibaba.fastjson.JSONObject;
import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.JDBC4Connection;
import com.mysql.jdbc.jdbc2.optional.ConnectionWrapper;
import com.mysql.jdbc.jdbc2.optional.JDBC4MysqlXAConnection;
import com.mysql.jdbc.jdbc2.optional.MysqlXAConnection;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.bytesoft.common.utils.Base64Util;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.resource.XATerminator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XATerminatorImpl implements XATerminator {
    static final Logger logger = LoggerFactory.getLogger(XATerminatorImpl.class);

    private TransactionBeanFactory beanFactory;
    private final List<XAResourceArchive> resources = new ArrayList<XAResourceArchive>();

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
                backInfo(archive);
            }

            logger.info("[{}] prepare: xares= {}, branch= {}, vote= {}",
                    ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
                    ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), archive.getVote());
        }

        return globalVote;
    }

    private void backInfo(XAResourceArchive archive) {


        if (archive.getDescriptor().getDelegate() instanceof JDBC4MysqlXAConnection) {
            System.out.println("begin mysql backInfo");
            MysqlXAConnection connection = (MysqlXAConnection) archive.getDescriptor().getDelegate();
            PreparedStatement ps = null;
            //拼接PREPARE语句，在general_log查找执行中的sql
            StringBuilder commandBuf = new StringBuilder(300);
            commandBuf.append("XA PREPARE ");
            appendXid(commandBuf, archive.getXid());
            String sqlStr = "select argument from mysql.general_log where thread_id = (select thread_id from mysql.general_log " +
                    "where argument = '" + commandBuf.toString() + "') and (argument like 'insert%' or argument like 'update%' or argument like 'delete%')";
            Statement stmt = null;
            ResultSet rs = null;
            List rollList = new ArrayList<String>();
            String backInfo = null;
            Connection conn = null;
            try {
                Connection myconn = connection.getConnection();
                ConnectionWrapper connwap = (ConnectionWrapper) myconn;

                Field mc = ConnectionWrapper.class.getDeclaredField("mc");
                mc.setAccessible(true);
                Object mymc = mc.get(connwap);
                ConnectionImpl j4conn = (JDBC4Connection) mymc;

                Field pass = ConnectionImpl.class.getDeclaredField("password");
                pass.setAccessible(true);
                Object passobj = pass.get(j4conn);
                String password = passobj.toString();

                Field fuser = ConnectionImpl.class.getDeclaredField("user");
                fuser.setAccessible(true);
                Object userobj = fuser.get(j4conn);
                String user = userobj.toString();


                DatabaseMetaData databaseMetaData = myconn.getMetaData();

                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection(databaseMetaData.getURL(), user, password);

                stmt = conn.createStatement();

                rs = stmt.executeQuery(sqlStr);
                while (rs.next()) {
                    rollList.add(rs.getString("argument"));
                }
                if (rollList.size() > 0) {
                    backInfo = handleRollBack(rollList, conn, stmt);
                }else
                {
                    backInfo = "warn:can not find exe sql,select sql ="+sqlStr ;
                }
                System.out.println("backInfo="+backInfo);
                String GloableXid = partGloableXid(archive.getXid());
                String branchXid = partBranchXid(archive.getXid());
                String logSql = "INSERT INTO txc_undo_log (gmt_create,gmt_modified,xid,branch_id,rollback_info,status,server) VALUES(now(),now(),?,?,?,?,?)";
                ps = conn.prepareStatement(logSql);
                ps.setString(1, GloableXid);
                ps.setString(2, branchXid);
                ps.setString(3, backInfo);
                ps.setInt(4, 0);
                ps.setString(5, "127.0.0.1");
                ps.executeUpdate();         //执行sql语句
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                //关闭自建创建的连接
                try {
                    // stmt.execute("set global general_log=off");
                    rs.close();
                    conn.close();
                    stmt.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            System.out.println("end mysql backInfo");

        }

    }

    private String partGloableXid(Xid xid) {

        byte[] gtrid = xid.getGlobalTransactionId();

        StringBuilder builder = new StringBuilder();

        if (gtrid != null) {
            appendAsHex(builder, gtrid);
        }

        return builder.toString();
    }

    private String partBranchXid(Xid xid) {

        byte[] btrid = xid.getBranchQualifier();

        StringBuilder builder = new StringBuilder();

        if (btrid != null) {
            appendAsHex(builder, btrid);
        }

        return builder.toString();
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

    private void invokeTwoPhaseCommit(XAResourceArchive archive) throws XAException {
        try {
            archive.commit(archive.getXid(), false);
        } catch (XAException xaex) {
            // * @exception XAException An error has occurred. Possible XAExceptions
            // * are XA_HEURHAZ, XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR,
            // * XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or XAER_PROTO.
            // * <P>If the resource manager did not commit the transaction and the
            // * parameter onePhase is set to true, the resource manager may throw
            // * one of the XA_RB* exceptions. Upon return, the resource manager has
            // * rolled back the branch's work and has released all held resources.
            switch (xaex.errorCode) {
                case XAException.XA_HEURHAZ:
                    // OSI-TP: The condition that arises when, as a result of communication failure with a
                    // subordinate, the bound data of the subordinate's subtree are in an unknown state.

                    // XA: Due to some failure, the work done on behalf of the specified
                    // transaction branch may have been heuristically completed.
                case XAException.XA_HEURMIX:
                    // Due to a heuristic decision, the work done on behalf of the specified
                    // transaction branch was partially committed and partially rolled back.
                case XAException.XA_HEURCOM:
                    // Due to a heuristic decision, the work done on behalf of
                    // the specified transaction branch was committed.
                case XAException.XA_HEURRB:
                    // Due to a heuristic decision, the work done on behalf of
                    // the specified transaction branch was rolled back.
                    throw xaex;
                case XAException.XAER_NOTA:
                    // The specified XID is not known by the resource manager.
                    throw new XAException(XAException.XA_RDONLY); // read-only
                case XAException.XAER_RMFAIL:
                    // An error occurred that makes the resource manager unavailable.
                    throw xaex;
                case XAException.XAER_INVAL:
                    // Invalid arguments were specified.
                case XAException.XAER_PROTO:
                    // The routine was invoked in an improper context.
                    XAException error = new XAException(XAException.XAER_RMERR);
                    error.initCause(xaex);
                    throw error;
                case XAException.XAER_RMERR:
                    // An error occurred in committing the work performed on behalf of the transaction
                    // branch and the branch’s work has been rolled back. Note that returning this error
                    // signals a catastrophic event to a transaction manager since other resource
                    // managers may successfully commit their work on behalf of this branch. This error
                    // should be returned only when a resource manager concludes that it can never
                    // commit the branch and that it cannot hold the branch’s resources in a prepared
                    // state. Otherwise, [XA_RETRY] should be returned.
                default:// XA_RB*
                    XAException xarb = new XAException(XAException.XA_HEURRB);
                    xarb.initCause(xaex);
                    throw xarb;
            }
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
        try {
            System.out.println("bengin invokeRollback");
            if (archive.getDescriptor().getDelegate() instanceof JDBC4MysqlXAConnection) {
                System.out.println("bengin  invokeRollback JDBC4MysqlXAConnection");
                MysqlXAConnection connection = (MysqlXAConnection) archive.getDescriptor().getDelegate();
                PreparedStatement ps = null;
                //拼接PREPARE语句，在general_log查找执行中的sql
                StringBuilder commandBuf = new StringBuilder(300);
                String GloableXid = partGloableXid(archive.getXid());
                String branchXid = partBranchXid(archive.getXid());
                String sqlStr = "select rollback_info from txc_undo_log where branch_id ='"+branchXid+"' and xid ='"+GloableXid+"'";
                Statement stmt = null;
                ResultSet rs = null;
                List rollList = new ArrayList<String>();
                List<String> backInfo = null;
                Connection conn = null;
                try {
                    Connection myconn = connection.getConnection();
                    ConnectionWrapper connwap = (ConnectionWrapper) myconn;

                    Field mc = ConnectionWrapper.class.getDeclaredField("mc");
                    mc.setAccessible(true);
                    Object mymc = mc.get(connwap);
                    ConnectionImpl j4conn = (JDBC4Connection) mymc;

                    Field pass = ConnectionImpl.class.getDeclaredField("password");
                    pass.setAccessible(true);
                    Object passobj = pass.get(j4conn);
                    String password = passobj.toString();

                    Field fuser = ConnectionImpl.class.getDeclaredField("user");
                    fuser.setAccessible(true);
                    Object userobj = fuser.get(j4conn);
                    String user = userobj.toString();


                    DatabaseMetaData databaseMetaData = myconn.getMetaData();

                    Class.forName("com.mysql.jdbc.Driver");
                    conn = DriverManager.getConnection(databaseMetaData.getURL(), user, password);

                    stmt = conn.createStatement();

                    rs = stmt.executeQuery(sqlStr);
                    while (rs.next()) {
                        rollList.add(rs.getString("rollback_info"));
                    }
                    if (rollList.size() > 0) {
                        System.out.println("bengin  invokeRollback rollbackinfo="+rs.getString("rollback_info"));
                        backInfo = decodeRollBackSql(rollList);
                        if(!rollback(backInfo, conn, stmt))
                        {
                            logger.error(String.format("Roll back mysql info error!,backInfo:s%",backInfo));
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    //关闭自建创建的连接
                    try {
                        rs.close();
                        conn.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    try {
                        stmt.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            } else {
                archive.rollback(archive.getXid());
            }
        } catch (XAException xaex) {
            // * @exception XAException An error has occurred. Possible XAExceptions are
            // * XA_HEURHAZ, XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR, XAER_RMFAIL,
            // * XAER_NOTA, XAER_INVAL, or XAER_PROTO.
            // * <p>If the transaction branch is already marked rollback-only the
            // * resource manager may throw one of the XA_RB* exceptions. Upon return,
            // * the resource manager has rolled back the branch's work and has released
            // * all held resources.
            switch (xaex.errorCode) {
                case XAException.XA_HEURHAZ:
                    // Due to some failure, the work done on behalf of the specified transaction branch
                    // may have been heuristically completed. A resource manager may return this
                    // value only if it has successfully prepared xid.
                case XAException.XA_HEURMIX:
                    // Due to a heuristic decision, the work done on behalf of the specified transaction
                    // branch was partially committed and partially rolled back. A resource manager
                    // may return this value only if it has successfully prepared xid.
                case XAException.XA_HEURCOM:
                    // Due to a heuristic decision, the work done on behalf of the specified transaction
                    // branch was committed. A resource manager may return this value only if it has
                    // successfully prepared xid.
                case XAException.XA_HEURRB:
                    // Due to a heuristic decision, the work done on behalf of the specified transaction
                    // branch was rolled back. A resource manager may return this value only if it has
                    // successfully prepared xid.
                    throw xaex;
                case XAException.XAER_RMFAIL:
                    // An error occurred that makes the resource manager unavailable.
                    XAException xrhaz = new XAException(XAException.XA_HEURHAZ);
                    xrhaz.initCause(xaex);
                    throw xrhaz;
                case XAException.XAER_NOTA:
                    // The specified XID is not known by the resource manager.
                    if (archive.isReadonly()) {
                        throw new XAException(XAException.XA_RDONLY);
                    } else if (archive.getVote() == XAResourceArchive.DEFAULT_VOTE) {
                        break; // rolled back
                    } else if (archive.getVote() == XAResource.XA_RDONLY) {
                        throw new XAException(XAException.XA_RDONLY);
                    } else if (archive.getVote() == XAResource.XA_OK) {
                        throw new XAException(XAException.XAER_RMERR);
                    } else {
                        throw new XAException(XAException.XAER_RMERR);
                    }
                case XAException.XAER_PROTO:
                    // The routine was invoked in an improper context.
                case XAException.XAER_INVAL:
                    // Invalid arguments were specified.
                    throw new XAException(XAException.XAER_RMERR);
                case XAException.XAER_RMERR:
                    // An error occurred in rolling back the transaction branch. The resource manager is
                    // free to forget about the branch when returning this error so long as all accessing
                    // threads of control have been notified of the branch’s state.
                default: // XA_RB*
                    // The resource manager has rolled back the transaction branch’s work and has
                    // released all held resources. These values are typically returned when the
                    // branch was already marked rollback-only.
                    XAException xarb = new XAException(XAException.XA_HEURRB);
                    xarb.initCause(xaex);
                    throw xarb;
            }
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


    private static void appendXid(StringBuilder builder, Xid xid) {
        byte[] gtrid = xid.getGlobalTransactionId();
        byte[] btrid = xid.getBranchQualifier();

        if (gtrid != null) {
            appendAsHex(builder, gtrid);
        }

        builder.append(',');
        if (btrid != null) {
            appendAsHex(builder, btrid);
        }

        builder.append(',');
        appendAsHex(builder, xid.getFormatId());
    }

    private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public static void appendAsHex(StringBuilder builder, byte[] bytes) {
        builder.append("0x");
        for (byte b : bytes) {
            builder.append(HEX_DIGITS[(b >>> 4) & 0xF]).append(HEX_DIGITS[b & 0xF]);
        }
    }


    public static void appendAsHex(StringBuilder builder, int value) {
        if (value == 0) {
            builder.append("0x0");
            return;
        }

        int shift = 32;
        byte nibble;
        boolean nonZeroFound = false;

        builder.append("0x");
        do {
            shift -= 4;
            nibble = (byte) ((value >>> shift) & 0xF);
            if (nonZeroFound) {
                builder.append(HEX_DIGITS[nibble]);
            } else if (nibble != 0) {
                builder.append(HEX_DIGITS[nibble]);
                nonZeroFound = true;
            }
        } while (shift != 0);
    }


    private String handleRollBack(List<String> list, Connection connection, Statement stmt) {
        List<String> backSql = new ArrayList<String>();
        for (String rollsql : list) {
            if (rollsql.startsWith("insert")) {
                backSql.addAll(handleInsert(rollsql));
            } else if (rollsql.startsWith("update")) {
                backSql.addAll(handleUpdate(rollsql, stmt, connection));
            } else if (rollsql.startsWith("delete")) {
                backSql.addAll(handleDelete(rollsql, connection));
            }
        }
        String backSqlJson = JSON.toJSONString(encode(backSql));
        return backSqlJson;

    }

    private List<String> handleInsert(String insertSql) {
        //todo 此处会有bug，改成有主键，按主键回滚，没有主键，放弃异步回滚
        try {
            String table = name_insert_table(insertSql);
            net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(insertSql);
            Insert insertStatement = (Insert) statement;
            StringBuilder whereSql = new StringBuilder();
            for (int i = 0; i < insertStatement.getColumns().size(); i++) {
                whereSql.append(" and " + insertStatement.getColumns().get(i).getColumnName() + "=" + ((ExpressionList) insertStatement.getItemsList()).getExpressions().get(i).toString());
            }
            final String backSql = "delete from " + table + " where 1=1 " + whereSql.toString();
            return new ArrayList<String>() {{
                add(backSql);
            }};
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<String>();

    }

    private List<String> handleUpdate(String updateSql, Statement stmt, Connection conn) {

        List<String> updateBackSql = new ArrayList<String>();
        List<Map<String, Object>> key_value_list = new ArrayList<Map<String, Object>>();
        try {

            List<String> column_list = name_update_column(updateSql);
            String columns = transList(column_list);
            List<String> tables = name_update_table(updateSql);
            if (tables.size() > 1) {
                logger.error("Unsupport multi tables for update");
                return null;
            }
            String pkey = getPrimaryKey(conn, tables.get(0))[0];
            columns = pkey + "," + columns;
            String table = tables.get(0);
            String whereSql = name_update_where(updateSql);
            String selectSql = "select " + columns + " from " + table + " where " + whereSql;
            logger.info(String.format("Save old data for update,data query sql =%s", selectSql));
            ResultSet rs = stmt.executeQuery(selectSql);
            while (rs.next()) {
                Map<String, Object> map = new HashMap<String, Object>();
                for (String col : column_list) {
                    map.put(col, rs.getObject(col));
                }
                map.put(pkey, rs.getObject(pkey));
                key_value_list.add(map);
            }

            for (Map<String, Object> peMap : key_value_list) {
                StringBuffer backSql = new StringBuffer();
                backSql.append("update ");
                backSql.append(table);
                backSql.append(" set ");
                for (String col : peMap.keySet()) {
                    if (!col.equals(pkey)) {
                        backSql.append(col + " = ");
                        Object obj = peMap.get(col);
                        if (obj instanceof String) {
                            backSql.append("'" + peMap.get(col) + "',");
                        } else {
                            backSql.append(peMap.get(col) + ",");
                        }
                    }
                }
                while (backSql.charAt(backSql.length() - 1) == ',') {
                    backSql.deleteCharAt(backSql.length() - 1);
                }
                backSql.append(" where ");
                Object obj = peMap.get(pkey);
                backSql.append(pkey + "=" + peMap.get(pkey));
                updateBackSql.add(backSql.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateBackSql;

    }

    private List<String> handleDelete(String deleteSql, Connection conn) {
        List<String> deleteBackList = new ArrayList<String>();
        List<Map<String, Object>> key_value_list = new ArrayList<Map<String, Object>>();
        try {
            List<String> column_list = new ArrayList<String>();
            List<String> tables = name_delete_table(deleteSql);
            if (tables.size() > 1) {
                throw new IllegalArgumentException("Unsupport multi tables for delete");
            }
            String table = tables.get(0);
            String whereSql = name_delete_where(deleteSql);
            String selectSql = "select * from " + table + " where " + whereSql;
            logger.info(String.format("Save old data for update,data query sql =%s", selectSql));
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql);
            ResultSetMetaData rsmd = rs.getMetaData();
            int count = rsmd.getColumnCount();
            String[] name = new String[count];
            for (int i = 0; i < count; i++) {
                column_list.add(rsmd.getColumnName(i + 1));
            }
            while (rs.next()) {
                Map<String, Object> map = new HashMap<String, Object>();
                for (String col : column_list) {
                    map.put(col, rs.getObject(col));
                }
                key_value_list.add(map);
            }

            for (Map<String, Object> peMap : key_value_list) {
                StringBuffer backSql = new StringBuffer();
                backSql.append("insert into ");
                backSql.append(table);
                backSql.append(" ( ");
                backSql.append(transList(column_list) + " )values(");
                for (String col : column_list) {
                    Object obj = peMap.get(col);
                    if (obj instanceof String) {
                        backSql.append("'" + obj + "',");
                    } else {
                        backSql.append(obj + ",");
                    }
                }
                while (backSql.charAt(backSql.length() - 1) == ',') {
                    backSql.deleteCharAt(backSql.length() - 1);
                }
                backSql.append(" ) ");
                deleteBackList.add(backSql.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return deleteBackList;
    }


    public static String[] getPrimaryKey(Connection con, String table) throws Exception {
        //mysql 获取主键的方法
        String sql = "SHOW CREATE TABLE " + table;
        try {
            PreparedStatement pre = con.prepareStatement(sql);
            ResultSet rs = pre.executeQuery();
            if (rs.next()) {
                //正则匹配数据
                Pattern pattern = Pattern.compile("PRIMARY KEY \\(\\`(.*)\\`\\)");
                Matcher matcher = pattern.matcher(rs.getString(2));
                matcher.find();
                String data = matcher.group();
                //过滤对于字符
                data = data.replaceAll("\\`|PRIMARY KEY \\(|\\)", "");
                //拆分字符
                String[] stringArr = data.split(",");
                return stringArr;
            }
        } catch (Exception e) {
            throw e;
        }
        return null;
    }

    public static List<String> encode(List<String> list) {
        //base64加密
        List<String> code_list = new ArrayList<String>();
        for (String str : list) {
            code_list.add(Base64Util.encode(str.getBytes()));
        }
        return code_list;
    }

    public static List<String> decode(List<String> list) {
        //base64加密
        List<String> code_list = new ArrayList<String>();
        for (String str : list) {
            try {
                code_list.add(new String(Base64Util.decode(str)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return code_list;
    }


    // ****insert table
    public static String name_insert_table(String sql)
            throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
        Insert insertStatement = (Insert) statement;
        String string_tablename = insertStatement.getTable().getName();
        return string_tablename;
    }

    // ********* insert table column
    public static List<String> name_insert_column(String sql)
            throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
        Insert insertStatement = (Insert) statement;
        List<Column> table_column = insertStatement.getColumns();
        List<String> str_column = new ArrayList<String>();
        for (int i = 0; i < table_column.size(); i++) {
            str_column.add(table_column.get(i).toString());
        }
        return str_column;
    }


    // ********* Insert values ExpressionList
    public static List<String> name_insert_values(String sql)
            throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
        Insert insertStatement = (Insert) statement;
        List<Expression> insert_values_expression = ((ExpressionList) insertStatement
                .getItemsList()).getExpressions();
        List<String> str_values = new ArrayList<String>();
        for (int i = 0; i < insert_values_expression.size(); i++) {
            str_values.add(insert_values_expression.get(i).toString());
        }
        return str_values;
    }

    // *********update table name
    public static List<String> name_update_table(String sql)
            throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
        Update updateStatement = (Update) statement;
        List<Table> update_table = updateStatement.getTables();
        List<String> str_table = new ArrayList<String>();
        if (update_table != null) {
            for (int i = 0; i < update_table.size(); i++) {
                str_table.add(update_table.get(i).toString());
            }
        }
        return str_table;

    }

    public static List<String> name_delete_table(String sql)
            throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
        Delete updateStatement = (Delete) statement;
        Table update_table = updateStatement.getTable();
        List<String> str_table = new ArrayList<String>();
        if (update_table != null) {
            str_table.add(update_table.toString());
        }
        return str_table;

    }

    // *********update column
    public static List<String> name_update_column(String sql)
            throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
        Update updateStatement = (Update) statement;
        List<Column> update_column = updateStatement.getColumns();
        List<String> str_column = new ArrayList<String>();
        if (update_column != null) {
            for (int i = 0; i < update_column.size(); i++) {
                str_column.add(update_column.get(i).toString());
            }
        }
        return str_column;

    }


    // *******update where
    public static String name_update_where(String sql)
            throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
        Update updateStatement = (Update) statement;
        Expression where_expression = updateStatement.getWhere();
        String str = where_expression.toString();
        return str;
    }

    // *******update where
    public static String name_delete_where(String sql)
            throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
        Delete updateStatement = (Delete) statement;
        Expression where_expression = updateStatement.getWhere();
        String str = where_expression.toString();
        return str;
    }

    public static String transList(List<String> list) {
        StringBuffer buf = new StringBuffer();
        for (String str : list) {
            buf.append(str).append(",");
        }
        while (buf.charAt(buf.length() - 1) == ',') {
            buf.deleteCharAt(buf.length() - 1);
        }
        return buf.toString();
    }


    private List<String> decodeRollBackSql(List<String> list) {
        List<String> backSql = new ArrayList<String>();
        for (String rollsql : list) {
            System.out.println("begin decodeRollBackSql rollsql");
            List<String> tmpsql = new ArrayList<String>();
            tmpsql = JSONObject.parseArray(rollsql, String.class);
            backSql.addAll(backSql);
        }
        backSql= decode(backSql);
        return backSql;

    }


    private boolean rollback(List<String> list, Connection connection, Statement stmt) {
        for (String rollsql : list) {
            try {
                System.out.println("exec back sql="+rollsql);
                stmt.execute(rollsql);
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true ;
    }

}
