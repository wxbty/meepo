package org.feisoft.jta;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.feisoft.common.utils.ByteUtils;
import org.feisoft.common.utils.DbPool.DbPoolUtil;
import org.feisoft.jta.image.BackInfo;
import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.transaction.*;
import org.feisoft.transaction.Transaction;
import org.feisoft.transaction.TransactionManager;
import org.feisoft.transaction.aware.TransactionBeanFactoryAware;
import org.feisoft.transaction.supports.TransactionTimer;
import org.feisoft.transaction.xa.TransactionXid;
import org.feisoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionManagerImpl implements TransactionManager, TransactionTimer, TransactionBeanFactoryAware {

    static final Logger logger = LoggerFactory.getLogger(TransactionManagerImpl.class);

    @javax.inject.Inject
    private TransactionBeanFactory beanFactory;

    private int timeoutSeconds = 5 * 6000;

    private final Map<Thread, Transaction> associatedTxMap = new ConcurrentHashMap<Thread, Transaction>();

    private int expireMilliSeconds = 15 * 1000;

    public void begin() throws NotSupportedException, SystemException {
        if (this.getTransaction() != null) {
            throw new NotSupportedException();
        }

        XidFactory xidFactory = this.beanFactory.getXidFactory();
        RemoteCoordinator transactionCoordinator = this.beanFactory.getTransactionCoordinator();

        int timeoutSeconds = this.timeoutSeconds;

        TransactionContext transactionContext = new TransactionContext();
        transactionContext.setPropagatedBy(transactionCoordinator.getIdentifier());
        transactionContext.setCoordinator(true);
        long createdTime = System.currentTimeMillis();
        long expiredTime = createdTime + (timeoutSeconds * 1000L);
        transactionContext.setCreatedTime(createdTime);
        transactionContext.setExpiredTime(expiredTime);

        TransactionXid globalXid = xidFactory.createGlobalXid();
        transactionContext.setXid(globalXid);

        TransactionImpl transaction = new TransactionImpl(transactionContext);
        transaction.setBeanFactory(this.beanFactory);
        transaction.setTransactionTimeout(this.timeoutSeconds);

        this.associateThread(transaction);
        TransactionRepository transactionRepository = this.beanFactory.getTransactionRepository();
        transactionRepository.putTransaction(globalXid, transaction);
        // this.transactionStatistic.fireBeginTransaction(transaction);

        logger.info("[{}] begin-transaction", ByteUtils.byteArrayToString(globalXid.getGlobalTransactionId()));
    }

    public void commit()
            throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException,
            IllegalStateException, SystemException {
        Transaction transaction = this.desociateThread();
        if (transaction == null) {
            throw new IllegalStateException();
        } else if (transaction.getTransactionStatus() == Status.STATUS_ROLLEDBACK) {
            throw new RollbackException();
        } else if (transaction.getTransactionStatus() == Status.STATUS_COMMITTED) {
            return;
        } else if (transaction.getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK) {
            this.rollback(transaction);
            throw new HeuristicRollbackException();
        } else if (transaction.getTransactionStatus() != Status.STATUS_ACTIVE) {
            throw new IllegalStateException();
        }

        TransactionRepository transactionRepository = this.beanFactory.getTransactionRepository();
        TransactionContext transactionContext = transaction.getTransactionContext();
        TransactionXid transactionXid = transactionContext.getXid();
        try {
            transaction.commit();
            transaction.forgetQuietly(); // forget transaction
        } catch (IllegalStateException ex) {
            logger.error("Error occurred while committing transaction.", ex);
            transactionRepository.putErrorTransaction(transactionXid, transaction);
            throw ex;
        } catch (SecurityException ex) {
            logger.error("Error occurred while committing transaction.", ex);
            transactionRepository.putErrorTransaction(transactionXid, transaction);
            throw ex;
        } catch (RollbackException rex) {
            logger.error("Error occurred while committing transaction.", rex);
            transaction.forgetQuietly(); // forget transaction
            throw rex;
        } catch (HeuristicMixedException hmex) {
            logger.error("Error occurred while committing transaction.", hmex);
            transaction.forgetQuietly(); // forget transaction
            throw hmex;
        } catch (HeuristicRollbackException hrex) {
            logger.error("Error occurred while committing transaction.", hrex);
            transaction.forgetQuietly(); // forget transaction
            throw hrex;
        } catch (SystemException ex) {
            logger.error("Error occurred while committing transaction.", ex);
            transactionRepository.putErrorTransaction(transactionXid, transaction);
            throw ex;
        } catch (RuntimeException rex) {
            logger.error("Error occurred while committing transaction.", rex);
            transactionRepository.putErrorTransaction(transactionXid, transaction);
            throw rex;
        }
    }

    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        Transaction transaction = this.desociateThread();
        this.rollback(transaction);
    }

    protected void rollback(Transaction transaction) throws IllegalStateException, SecurityException, SystemException {
        if (transaction == null) {
            throw new IllegalStateException();
        } else if (transaction.getTransactionStatus() == Status.STATUS_ROLLEDBACK) {
            return;
        } else if (transaction.getTransactionStatus() == Status.STATUS_COMMITTED) {
            throw new SystemException();
        }

        TransactionRepository transactionRepository = this.beanFactory.getTransactionRepository();
        TransactionContext transactionContext = transaction.getTransactionContext();
        TransactionXid transactionXid = transactionContext.getXid();
        try {
            transaction.rollback();
            transaction.forgetQuietly();
        } catch (IllegalStateException ex) {
            logger.error("Error occurred while rolling back transaction.", ex);
            transactionRepository.putErrorTransaction(transactionXid, transaction);
            throw ex;
        } catch (SecurityException ex) {
            logger.error("Error occurred while rolling back transaction.", ex);
            transactionRepository.putErrorTransaction(transactionXid, transaction);
            throw ex;
        } catch (SystemException ex) {
            logger.error("Error occurred while rolling back transaction.", ex);
            transactionRepository.putErrorTransaction(transactionXid, transaction);
            throw ex;
        } catch (RuntimeException ex) {
            logger.error("Error occurred while rolling back transaction.", ex);
            transactionRepository.putErrorTransaction(transactionXid, transaction);
            throw ex;
        }
    }

    public void associateThread(Transaction transaction) {
        this.associatedTxMap.put(Thread.currentThread(), transaction);
    }

    public Transaction desociateThread() {
        return this.associatedTxMap.remove(Thread.currentThread());
    }

    public Transaction suspend() throws RollbackRequiredException, SystemException {
        Transaction transaction = this.desociateThread();
        transaction.suspend();
        return transaction;
    }

    public void resume(javax.transaction.Transaction tobj)
            throws InvalidTransactionException, IllegalStateException, RollbackRequiredException, SystemException {

        if (TransactionImpl.class.isInstance(tobj) == false) {
            throw new InvalidTransactionException();
        } else if (this.getTransaction() != null) {
            throw new IllegalStateException();
        }

        TransactionImpl transaction = (TransactionImpl) tobj;
        transaction.resume();
        this.associateThread(transaction);

    }

    public int getStatus() throws SystemException {
        Transaction transaction = this.getTransaction();
        return transaction == null ? Status.STATUS_NO_TRANSACTION : transaction.getTransactionStatus();
    }

    public Transaction getTransaction(Thread thread) {
        return this.associatedTxMap.get(thread);
    }

    public Transaction getTransactionQuietly() {
        try {
            return this.getTransaction();
        } catch (SystemException ex) {
            return null;
        } catch (RuntimeException ex) {
            return null;
        }
    }

    public Transaction getTransaction() throws SystemException {
        return this.associatedTxMap.get(Thread.currentThread());
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException {
        Transaction transaction = this.getTransaction();
        if (transaction == null) {
            throw new SystemException();
        }
        transaction.setRollbackOnly();
    }

    public void setTransactionTimeout(int seconds) throws SystemException {
        Transaction transaction = this.getTransaction();
        if (transaction == null) {
            throw new SystemException();
        } else if (seconds < 0) {
            throw new SystemException();
        } else if (seconds == 0) {
            // ignore
        } else {
            ((TransactionImpl) transaction).changeTransactionTimeout(seconds * 1000);
        }
    }

    public void timingExecution() {
        List<Transaction> expiredTransactions = new ArrayList<Transaction>();
        List<Transaction> activeTransactions = new ArrayList<Transaction>(this.associatedTxMap.values());
        long current = System.currentTimeMillis();
        Iterator<Transaction> activeItr = activeTransactions.iterator();
        while (activeItr.hasNext()) {
            Transaction transaction = activeItr.next();
            if (transaction.isTiming()) {
                TransactionContext transactionContext = transaction.getTransactionContext();
                if (transactionContext.getExpiredTime() <= current) {
                    expiredTransactions.add(transaction);
                }
            } // end-if (transaction.isTiming())
        }

        Iterator<Transaction> expiredItr = expiredTransactions.iterator();
        while (activeItr.hasNext()) {
            Transaction transaction = expiredItr.next();
            if (transaction.getTransactionStatus() == Status.STATUS_ACTIVE
                    || transaction.getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK) {
                this.timingRollback(transaction);
            }
        }

        //清除超期lock
        if (DbPoolUtil.isInited()) {
            rollbackOverTimeImage();
        }

    }

    private void rollbackOverTimeImage() {

        long now = System.currentTimeMillis();
        logger.debug("TransactionManagerImpl.rollbackOverTimeImage,now={}", now);
        try {
            String sql =
                    "select u.id, rollback_info,k.create_time from txc_lock k,txc_undo_log u where k.xid=u.xid and k.branch_id=u.branch_id and  k.create_time +"
                            + expireMilliSeconds + "< " + now;
            AtomicBoolean isExpire = new AtomicBoolean(false);
            DbPoolUtil.executeQuery(sql, rs -> {
                Long nowMillis = System.currentTimeMillis();
                Long txMillis = rs.getLong("create_time");
                if (nowMillis - txMillis < timeoutSeconds) {
                    logger.debug("Not go to expired txc,continue!");
                    return null;
                }

                isExpire.set(true);
                String imageInfo = rs.getString("rollback_info");
                logger.info("TransactionManagerImpl.ExeBackinfo:{}", imageInfo);

                BackInfo backInfo = JSON.parseObject(imageInfo, new TypeReference<BackInfo>() {

                });
                backInfo.setId(rs.getLong("id"));
                backInfo.rollback();
                backInfo.updateStatusFinish();
                return null;
            }, null);

            if (isExpire.get()) {
                sql = "delete from txc_lock where  create_time +" + expireMilliSeconds + "< " + now;
                DbPoolUtil.executeUpdate(sql);
            }
        } catch (SQLException e) {
            logger.error("SQLException", e);
        }

    }

    private void timingRollback(Transaction transaction) {
        TransactionContext transactionContext = transaction.getTransactionContext();
        TransactionXid globalXid = transactionContext.getXid();
        TransactionRepository transactionRepository = this.beanFactory.getTransactionRepository();

        try {
            transaction.rollback();
            transaction.forgetQuietly(); // forget transaction
        } catch (Exception ex) {
            transactionRepository.putErrorTransaction(globalXid, transaction);
        }

    }

    public void stopTiming(Transaction transaction) {
        if (TransactionImpl.class.isInstance(transaction)) {
            ((TransactionImpl) transaction).stopTiming();
        }
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public void setBeanFactory(TransactionBeanFactory tbf) {
        this.beanFactory = tbf;
    }

}
