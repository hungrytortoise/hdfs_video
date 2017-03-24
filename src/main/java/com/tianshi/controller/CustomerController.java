package com.tianshi.controller;

import com.tianshi.base.baseController;
import com.tianshi.domain.Customer;
import com.tianshi.hbase.HbaseClient;
import com.tianshi.service.CustomerService;
import com.tianshi.tools.Jlog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * Created by hefg on 2017/3/8.s
 */
@Controller
@RequestMapping("/customer")
public class CustomerController extends baseController {

        @Resource(name ="customerService")
        private CustomerService customerService; //这边注意  customerService 对应后面的 @Service("customerService") 一致
        //http://localhost:8080/customer/getbyid?id=1 访问这个
        @RequestMapping("/getbyid")
        @ResponseBody
        public String toIndex(HttpServletRequest request,Model model){
            //int id2 =Integer.parseInt(id);
            String id = request.getParameter("id") ;
            int id2 =Integer.parseInt(id) ;
            Customer customer = this.customerService.getCustomerById(id2);
            model.addAttribute("user", customer.getId());
            this.responseObj.put("id",customer.getId()) ;
            this.responseObj.put("age",customer.getAge()) ;
            this.responseObj.put("username",customer.getUserName()) ;
            Jlog.info("fangwendaole ");
            return this.sendResponse();

        }
         public void testHbase() throws IOException {
             HbaseClient hbaseClient = new  HbaseClient() ;
             String [] cols = new String[]{"id,name"} ;
             hbaseClient.createTable("mytesttable",cols);
             hbaseClient.close();
         }

}
