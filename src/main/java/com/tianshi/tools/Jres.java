package com.tianshi.tools;



import net.sf.json.JSONObject;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Jres {
	/**
	 * 所有的路由最终返回前端时，调用该方法，输出json串
	 * @param out
	 * @return
	 */
	public static String sendJson(Object out){
		return Jres.sendJson(0, "succ", out);
	}
	public static String sendJson(String msg,Object out){
		return Jres.sendJson(0, msg, out);
	}
	public static String sendJson(int code,String msg,Object out){
		JSONObject obj = new JSONObject();
		JSONObject oldobj = null;
		try{
			oldobj =  JSONObject.fromObject(out);
		}catch(Exception ee){
			
		}
		if(oldobj != null){
			if(oldobj.containsKey("ret_code")){
				code = oldobj.getInt("ret_code");
			}
			if(oldobj.containsKey("ret_msg")){
				msg = oldobj.getString("ret_msg");
				out = "";
			}
		}
		obj.put("requestid", Jlog.requestid);
		obj.put("ret_code", code);
		obj.put("ret_msg", msg);
		obj.put("data",out);
		return obj.toString();
	}
	
	public static String getNumFromStr(String str){
		Pattern p=Pattern.compile("(\\d+)");
		String num = null;
		Matcher m=p.matcher(str);
		if(m.find()){
				num = m.group(1);
		}
		return num;
	}
	
	public static Set<String> getKeyList(Set<String> set){
		Set<String> list = new HashSet<String>();
		for(String key : set){
			Jlog.info("my key:"+key);
			list.add(Jres.getNumFromStr(key));
			Jlog.info("my new key:"+Jres.getNumFromStr(key));
		}
		return list;
	}
	
	public static String replaceSubstr(String str,String substr,String newstr){
		return str.replace(substr, newstr);
	}
	
	public static JSONObject getNend(JSONObject j,int n){
		if(j.size() > 0){
			if(n < 0){
				n = n + j.size();
			}
			Iterator it = j.keys();
			int i = 0;
			while (it.hasNext()) {
				String key = (String) it.next(); 
                if(i == n){
                	j = (JSONObject) j.get(key);
                	break;
                }
                i++;
			}
		}
		return j;
	}
	

}

abstract class res_data{

}

