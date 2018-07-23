package com.bytesvc.comsumerService;

import com.bytesvc.ServiceException;
import com.bytesvc.service.IAccountService;
import com.bytesvc.service.ITransferService;
import com.bytesvc.service.mapper.AccountTwoDao;
import com.bytesvc.service.pojo.Account;
import org.springframework.beans.factory.annotation.Autowired;
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
	@Autowired
	private AccountTwoDao accountTwoDao;

//	@Transactional(rollbackFor = ServiceException.class)
	public void transfer(String sourceAcctId, String targetAcctId, double amount) throws ServiceException {

		this.increaseAmount(targetAcctId, amount);
//		this.remoteAccountService.decreaseAmount(sourceAcctId, amount);


//   		 throw new ServiceException("rollback");
	}

	@Transactional(rollbackFor = ServiceException.class)
	public  int getSum()
	{
		return this.remoteAccountService.getSum();
	}

	private void increaseAmount(String acctId, double amount) throws ServiceException {
//		int value = this.jdbcTemplate.update("update tb_account_two set amount = amount + "+amount+" where acct_id ='"+acctId+"'");
//		int value = this.jdbcTemplate.update("update tb_account_two set amount = ? where acct_id = ?", amount, acctId);
//		int value = this.jdbcTemplate.update("insert into orders(user_id,product_id,number,gmt_create) values('09616e24-5818-4784-8345-5a61e73d1478', 723, 3, '2018-07-20 16:01:33.747')");
//		String sql = "select name from apple where id =1  lock in  share mode ";
//		 //        调用方法获得记录数
//		String count = jdbcTemplate.queryForObject(sql, String.class);
//		System.out.println("count="+count);
		Account account = new Account();
		account.setAcctId(acctId);
		account.setAmount(amount);
		System.out.println("update num="+accountTwoDao.update(account));
		System.out.printf("exec increase: acct= %s, amount= %7.2f%n", acctId, amount);
	}

}
