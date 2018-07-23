package com.bytesvc.service.main;

import com.bytesvc.service.mapper.AccountDao;
import com.bytesvc.service.pojo.Account;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ProviderMain {

    static ClassPathXmlApplicationContext context = null;

    @Autowired
    private AccountDao accountDao;

    public static void main(String... args) throws Throwable {
        System.out.println("------begin start provider main----------");
        context = new ClassPathXmlApplicationContext("application.xml");
        Account account = new Account();
        account.setAcctId("3001");
        account.setAmount(2332.3);
//        accountDao.add(account);
        context.start();
        System.out.println("sample-provider started!");
    }

}
