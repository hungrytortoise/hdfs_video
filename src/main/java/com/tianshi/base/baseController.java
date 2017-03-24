/**
 * 
 */
/**
 * @author xierl
 *
 */
package com.tianshi.base;


import com.tianshi.tools.Jlog;
import com.tianshi.tools.Jreq;
import com.tianshi.tools.Jres;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

@Controller
public class baseController {
	
	protected Map<String,String> traceLog;
	protected JSONObject responseObj;
	protected JSONObject authInfo;
	
	@Autowired(required = false)
	public HttpServletRequest request;

	
	//@Autowired
	//protected  HttpServletResponse response;
	
	public baseController(){
		//init
		traceLog = new HashMap<String,String>();
	    responseObj = new JSONObject();
	    authInfo = new JSONObject();
	    Jlog.info("start request...");
	    //System.setProperty("user.timezone","GMT +08");
	}

	


	
	public void setResponseObj(JSONObject res){
		this.responseObj = res;
	}
	
	
	public String sendResponse(Object response,String callback){
		String res = "";
		if(this.get("callback",null) != null){
			callback = request.getParameter("callback");
		}
		if(callback == null){
			res = Jres.sendJson(response);
		}else{
			res = callback + "(" + Jres.sendJson(response) + ")";
		}
		this.responseObj.clear();
		Jlog.info("request end.==============qiong...");
		Jlog.requestid = "";
		return res;
	}
	
	public String sendResponse(Object response){
		return this.sendResponse(response,null);
	}
	
	public String sendResponse(){
		return this.sendResponse(this.responseObj,null);
	}
	
	public void log(String msg){
		StringBuffer logstr = new StringBuffer();
		Iterator<Entry<String, String>> entries = traceLog.entrySet().iterator(); 
		while (entries.hasNext()) {  
		    Entry<String, String> entry = entries.next();  
		    logstr.append(" [" + entry.getKey() + ":" + entry.getValue() + "] ");  
		}
		logstr.append(msg);
		Jlog.info(logstr);
	}
	
	public void debugLog(String msg){
		log(msg);
	}
	
	public void appendLog(String key,String value){
		traceLog.put(key, value);
	}
	
	public String get(String key,String defaultValue){
		String value = request.getParameter(key);
		if(value == null){
			value = defaultValue;
		}
		return value;
	}
	
	public String get(String key){
		return this.get(key,"");
	}

}