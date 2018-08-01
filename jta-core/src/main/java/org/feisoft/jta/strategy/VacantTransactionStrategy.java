/**
 * Copyright 2014-2017 yangming.liu<bytefox@126.com>.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, see <http://www.gnu.org/licenses/>.
 */
package org.feisoft.jta.strategy;

import org.feisoft.jta.TransactionStrategy;
import org.feisoft.transaction.CommitRequiredException;
import org.feisoft.transaction.RollbackRequiredException;

import javax.transaction.HeuristicCommitException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class VacantTransactionStrategy implements TransactionStrategy {

	public int start(Xid xid) throws RollbackRequiredException, CommitRequiredException {
		return 0;
	}

	public int prepare(Xid xid) throws RollbackRequiredException, CommitRequiredException {
		return XAResource.XA_RDONLY;
	}

	public void commit(Xid xid)
			throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, SystemException {
	}

	public void rollback(Xid xid)
			throws HeuristicMixedException, HeuristicCommitException, IllegalStateException, SystemException {
	}

}
