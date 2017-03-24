package com.tianshi.dao;

import com.tianshi.domain.Customer;

public interface CustomerDao {
    int deleteByPrimaryKey(Integer id);

    int insert(Customer record);

    int insertSelective(Customer record);

    Customer selectByPrimaryKey(Integer id);
    Customer getCustomerById (Integer  id) ;

    int updateByPrimaryKeySelective(Customer record);

    int updateByPrimaryKey(Customer record);
}