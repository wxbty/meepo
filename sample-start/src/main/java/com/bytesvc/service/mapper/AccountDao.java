package com.bytesvc.service.mapper;

import com.bytesvc.service.pojo.Account;
import org.springframework.stereotype.Component;

@Component
public interface AccountDao {

    public int add(Account account);
}
