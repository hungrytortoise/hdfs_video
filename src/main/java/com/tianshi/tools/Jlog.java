package com.tianshi.tools;

import org.apache.log4j.Logger;

import java.util.UUID;

public class Jlog {
	
	public static Logger logger;
	public static String methodname="";
	public static String filename = "";
	public static int linenum = 0;
	public static String classname = "";
	public static String requestid = "";
	public static long runtime ,starttime;
	
	
	public static Logger _get(){
		StackTraceElement stack[] = (new Throwable()).getStackTrace();
		int i = 3;	//this is the caller function
		classname = stack[i].getClassName();
		methodname = stack[i].getMethodName();
		filename = stack[i].getFileName();
		linenum = stack[i].getLineNumber();
		logger = Logger.getLogger(classname);
		if(requestid == ""){
			requestid=UUID.randomUUID().toString().replace("-", "");
			starttime = System.currentTimeMillis();
		}
		return logger;	
	}
	public static void _print(int type,Object msg){
		Logger logger = _get();
		boolean is_string = msg instanceof String;
		if( is_string == false){
			msg = msg.toString();
		}
		runtime = System.currentTimeMillis()-starttime;
		msg = ""+linenum+"-[requestid:"+requestid+"] "+"-[runtime:"+ runtime +"] "+ msg.toString();
		switch(type){
		case 0:
			logger.debug(msg);
			break;
		case 1:
			logger.info(msg);
			break;
		case 2:
			logger.error(msg);
			break;
		case 3:
			logger.fatal(msg);
			break;
		}
	}
	
	public static void debug(Object msg){
		_print(0,msg);
	}
	public static void info(Object msg){
		_print(1,msg);
	}
	public static void error(Object msg){
		_print(2,msg);
	}
	public static void fatal(Object msg){
		_print(3,msg);
	}

}
