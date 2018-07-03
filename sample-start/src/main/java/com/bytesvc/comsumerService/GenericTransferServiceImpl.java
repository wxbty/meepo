package com.bytesvc.comsumerService;

import com.bytesvc.ServiceException;
import com.bytesvc.service.ITransferService;
import com.bytesvc.service.IAccountService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service("genericTransferService")
public class GenericTransferServiceImpl implements ITransferService {

	@javax.annotation.Resource(name = "jdbcTemplate2")
	private JdbcTemplate jdbcTemplate;
	@org.springframework.beans.factory.annotation.Qualifier("remoteAccountService")
	@org.springframework.beans.factory.annotation.Autowired(required = false)
	private IAccountService remoteAccountService;

	@Transactional(rollbackFor = ServiceException.class)
	public void transfer(String sourceAcctId, String targetAcctId, double amount) throws ServiceException {

		this.remoteAccountService.decreaseAmount(sourceAcctId, amount);
		this.increaseAmount(targetAcctId, amount);

		// throw new ServiceException("rollback");
	}

	private void increaseAmount(String acctId, double amount) throws ServiceException {
		int value = this.jdbcTemplate.update("update tb_account_two2 set amount = amount + ? where acct_id = ?", amount, acctId);

		System.out.printf("exec increase: acct= %s, amount= %7.2f%n", acctId, amount);
	}

}
