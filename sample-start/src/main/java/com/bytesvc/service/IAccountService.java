package com.bytesvc.service;

import com.bytesvc.ServiceException;

public interface IAccountService {

	public void increaseAmount(String accountId, double amount) throws ServiceException;

	public void decreaseAmount(String accountId, double amount) throws ServiceException;

}
