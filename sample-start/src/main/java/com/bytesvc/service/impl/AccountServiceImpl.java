package com.bytesvc.service.impl;

import com.bytesvc.ServiceException;
import com.bytesvc.service.IAccountService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service("accountService")
public class AccountServiceImpl implements IAccountService {

	@javax.annotation.Resource(name = "jdbcTemplate")
	private JdbcTemplate jdbcTemplate;

	@Transactional(rollbackFor = ServiceException.class)
	public void increaseAmount(String acctId, double amount) throws ServiceException {
		int value = this.jdbcTemplate.update("update tb_account_one set amount = amount + ? where acct_id = ?", amount, acctId);
		System.out.printf("exec increase: acct= %s, amount= %7.2f%n", acctId, amount);
	}

	@Transactional(rollbackFor = ServiceException.class)
	public void decreaseAmount(String acctId, double amount) throws ServiceException {
		int value = this.jdbcTemplate.update("update tb_account_one set amount = amount - ? where acct_id = ?", amount, acctId);
//		String sql = "select name from student where id =1 ";
		//        调用方法获得记录数
//		String count = jdbcTemplate.queryForObject(sql, String.class);
//		System.out.println("count="+count);
		System.out.printf("exec decrease: acct= %s, amount= %7.2f%n", acctId, amount);
		// throw new ServiceException("rollback");
	}


	public  int getSum()
	{
		String sql = "select count(0) from student where id =1 ";
		Integer count = jdbcTemplate.queryForObject(sql, Integer.class);
		return count;
	}

}
