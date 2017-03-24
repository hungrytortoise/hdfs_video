package com.tianshi.service.impl;

import com.tianshi.dao.CustomerDao;
import com.tianshi.domain.Customer;
import com.tianshi.service.CustomerService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * Created by hefg on 2017/3/8.
 */
@Service("customerService")
public class CustomerServiceImpl implements CustomerService{

@Resource
private  CustomerDao customerDao  ;
    public Customer getCustomerById(Integer id) {
        return customerDao.getCustomerById(id);
    }
}
