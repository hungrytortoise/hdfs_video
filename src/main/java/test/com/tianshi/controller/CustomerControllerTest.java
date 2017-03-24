package test.com.tianshi.controller; 

import com.sun.xml.internal.ws.dump.LoggingDumpTube;
import com.tianshi.controller.CustomerController;
import com.tianshi.hbase.HbaseClient;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import static com.sun.xml.internal.ws.dump.LoggingDumpTube.Position.After;
import static com.sun.xml.internal.ws.dump.LoggingDumpTube.Position.Before;

/** 
* CustomerController Tester. 
* 
* @author <Authors name> 
* @since <pre>���� 17, 2017</pre> 
* @version 1.0 
*/ 
public class CustomerControllerTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: toIndex(HttpServletRequest request, Model model) 
* 
*/ 
@Test
public void testToIndex() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: testHbase() 
* 
*/ 
@Test
public void testTestHbase() throws Exception { 
//TODO: Test goes here...
    CustomerController c = new CustomerController() ;
    c.testHbase();


} 


} 
