
package org.feisoft.jta.logging;

import org.apache.commons.lang3.StringUtils;
import org.feisoft.jta.logging.store.VirtualLoggingSystemImpl;
import org.feisoft.transaction.TransactionBeanFactory;
import org.feisoft.transaction.archive.TransactionArchive;
import org.feisoft.transaction.archive.XAResourceArchive;
import org.feisoft.transaction.aware.TransactionBeanFactoryAware;
import org.feisoft.transaction.aware.TransactionEndpointAware;
import org.feisoft.transaction.logging.ArchiveDeserializer;
import org.feisoft.transaction.logging.LoggingFlushable;
import org.feisoft.transaction.logging.TransactionLogger;
import org.feisoft.transaction.logging.store.VirtualLoggingListener;
import org.feisoft.transaction.logging.store.VirtualLoggingRecord;
import org.feisoft.transaction.recovery.TransactionRecoveryCallback;
import org.feisoft.transaction.xa.TransactionXid;
import org.feisoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SampleTransactionLogger extends VirtualLoggingSystemImpl
		implements TransactionLogger, LoggingFlushable, TransactionBeanFactoryAware, TransactionEndpointAware {
	static final Logger logger = LoggerFactory.getLogger(SampleTransactionLogger.class);

	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;
	private String endpoint;

	public void createTransaction(TransactionArchive archive) {
		ArchiveDeserializer deserializer = this.beanFactory.getArchiveDeserializer();

		try {
			byte[] byteArray = deserializer.serialize((TransactionXid) archive.getXid(), archive);
			this.create(archive.getXid(), byteArray);
		} catch (RuntimeException rex) {
			logger.error("Error occurred while creating transaction-archive.", rex);
		}
	}

	public void updateTransaction(TransactionArchive archive) {
		ArchiveDeserializer deserializer = this.beanFactory.getArchiveDeserializer();

		try {
			byte[] byteArray = deserializer.serialize((TransactionXid) archive.getXid(), archive);
			this.modify(archive.getXid(), byteArray);
		} catch (RuntimeException rex) {
			logger.error("Error occurred while modifying transaction-archive.", rex);
		}
	}

	public void deleteTransaction(TransactionArchive archive) {
		try {
			this.delete(archive.getXid());
		} catch (RuntimeException rex) {
			logger.error("Error occurred while deleting transaction-archive.", rex);
		}
	}

	public void updateResource(XAResourceArchive archive) {
		ArchiveDeserializer deserializer = this.beanFactory.getArchiveDeserializer();

		try {
			byte[] byteArray = deserializer.serialize((TransactionXid) archive.getXid(), archive);
			this.modify(archive.getXid(), byteArray);
		} catch (RuntimeException rex) {
			logger.error("Error occurred while modifying resource-archive.", rex);
		}
	}

	public void recover(TransactionRecoveryCallback callback) {

		final Map<Xid, TransactionArchive> xidMap = new HashMap<Xid, TransactionArchive>();
		final ArchiveDeserializer deserializer = this.beanFactory.getArchiveDeserializer();
		final XidFactory xidFactory = this.beanFactory.getXidFactory();

		this.traversal(new VirtualLoggingListener() {
			public void recvOperation(VirtualLoggingRecord action) {
				Xid xid = action.getIdentifier();
				int operator = action.getOperator();
				if (OPERATOR_DELETE == operator) {
					xidMap.remove(xid);
				} else if (xidMap.containsKey(xid) == false) {
					xidMap.put(xid, null);
				}
			}
		});

		this.traversal(new VirtualLoggingListener() {
			public void recvOperation(VirtualLoggingRecord action) {
				Xid xid = action.getIdentifier();
				if (xidMap.containsKey(xid)) {
					this.execOperation(action);
				}
			}

			public void execOperation(VirtualLoggingRecord action) {
				Xid identifier = action.getIdentifier();

				TransactionXid xid = xidFactory.createGlobalXid(identifier.getGlobalTransactionId());

				Object obj = deserializer.deserialize(xid, action.getValue());
				if (TransactionArchive.class.isInstance(obj)) {
					TransactionArchive archive = (TransactionArchive) obj;
					xidMap.put(identifier, archive);
				} else if (XAResourceArchive.class.isInstance(obj)) {
					TransactionArchive archive = xidMap.get(identifier);
					if (archive == null) {
						logger.error("Error occurred while recovering resource archive: {}", obj);
						return;
					}

					XAResourceArchive resourceArchive = (XAResourceArchive) obj;
					boolean matched = false;

					List<XAResourceArchive> nativeResources = archive.getNativeResources();
					for (int i = 0; matched == false && nativeResources != null && i < nativeResources.size(); i++) {
						XAResourceArchive element = nativeResources.get(i);
						if (resourceArchive.getXid().equals(element.getXid())) {
							matched = true;
							nativeResources.set(i, resourceArchive);
						}
					}

					XAResourceArchive optimizedResource = archive.getOptimizedResource();
					if (matched == false && optimizedResource != null) {
						if (resourceArchive.getXid().equals(optimizedResource.getXid())) {
							matched = true;
							archive.setOptimizedResource(resourceArchive);
						}
					}

					List<XAResourceArchive> remoteResources = archive.getRemoteResources();
					for (int i = 0; matched == false && remoteResources != null && i < remoteResources.size(); i++) {
						XAResourceArchive element = remoteResources.get(i);
						if (resourceArchive.getXid().equals(element.getXid())) {
							matched = true;
							remoteResources.set(i, resourceArchive);
						}
					}

					if (matched == false) {
						logger.error("Error occurred while recovering resource archive: {}, invalid resoure!", obj);
					}

				}

			}
		});

		for (Iterator<Map.Entry<Xid, TransactionArchive>> itr = xidMap.entrySet().iterator(); itr.hasNext();) {
			Map.Entry<Xid, TransactionArchive> entry = itr.next();
			TransactionArchive archive = entry.getValue();
			if (archive == null) {
				continue;
			} else {
				try {
					callback.recover(archive);
				} catch (RuntimeException rex) {
					logger.error("Error occurred while recovering transaction(xid= {}).", archive.getXid(), rex);
				}
			}
		}

	}

	public File getDefaultDirectory() {
		String address = StringUtils.trimToEmpty(this.endpoint);
		File directory = new File(String.format("jta/%s", address.replaceAll("[^a-zA-Z_0-9]", "_")));
		if (directory.exists() == false) {
			try {
				directory.mkdirs();
			} catch (SecurityException ex) {
				logger.error("Error occurred while creating directory {}!", directory.getAbsolutePath(), ex);
			}
		}
		return directory;
	}

	public int getMajorVersion() {
		return 0;
	}

	public int getMinorVersion() {
		return 3;
	}

	public String getLoggingFilePrefix() {
		return "jta-";
	}

	public String getLoggingIdentifier() {
		return "org.feisoft.jta.logging.sample";
	}

	public void setEndpoint(String identifier) {
		this.endpoint = identifier;
	}

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

}
