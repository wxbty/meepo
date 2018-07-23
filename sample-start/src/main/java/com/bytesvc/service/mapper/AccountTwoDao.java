package com.bytesvc.service.mapper;

import com.bytesvc.service.pojo.Account;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface AccountTwoDao {

    public int add(Account account);


    public int update(Account account);

    public List<Account> getAll();

    public int delete(int id);
}
