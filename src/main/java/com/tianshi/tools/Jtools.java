package com.tianshi.tools;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 基本工具
 * @author huangl
 *
 */
public class Jtools {
	public static void setLocal(){
		Locale.setDefault(new Locale(Locale.CHINA.toString()));
		//TimeZone timeZoneSH = TimeZone.getTimeZone("GMT+8");
	}
	/**
	 * 从字符串中，获取时间戳
	 * @param stime
	 * @return
	 */
	public static String getTimeByStr(String stime){
		return getTimeByStr(stime,"yyyy/MM/dd");
		
	}
	public static String getTimeByStr(String stime,String format){
		String re_time = "0";  
		SimpleDateFormat sdf = new SimpleDateFormat(format); 
		Date d;  
		try {  
		  
			d = sdf.parse(stime);  
			long l = d.getTime();  
			String str = String.valueOf(l);  
			re_time = str.substring(0, 10);   
		  
		} catch (ParseException e) {  
			Jlog.info("getTimebyStr meet exception"+e.getMessage());
		}  
		return re_time; 
	}
	/**
	 * 时间戳转换成字符串
	 * @param
	 * @return
	 */
	public static String getStrTime(String ctime){   
		return getStrTime(ctime,"yyyy/MM/dd");  
	}
	public static String getHourStrTime(String ctime){   
		return getStrTime(ctime,"yyyy/MM/dd HH:mm:ss");  
	}
	public static String getStrTime(String ctime,String format){
		String re_StrTime = null;  
		SimpleDateFormat sdf = new SimpleDateFormat(format);  
		long lcc_time = Long.valueOf(ctime);  
		re_StrTime = sdf.format(new Date(lcc_time * 1000L));    
		return re_StrTime;  
	}
	/**
	 * 当天0点时间戳
	 * @return
	 */
	public static int getDayBeginTime(){
		Calendar calendar = Calendar.getInstance(); 
		calendar.set(Calendar.HOUR_OF_DAY, 0); 
		calendar.set(Calendar.SECOND, 0); 
		calendar.set(Calendar.MINUTE, 0); 
		calendar.set(Calendar.MILLISECOND, 0); 
		return (int)(calendar.getTimeInMillis()/1000L); 
	}
	public static int getDayEndTIme(){
		Calendar calendar = Calendar.getInstance(); 
		calendar.set(Calendar.HOUR_OF_DAY, 24); 
		calendar.set(Calendar.SECOND, 0); 
		calendar.set(Calendar.MINUTE, 0); 
		calendar.set(Calendar.MILLISECOND, 0); 
		return (int)(calendar.getTimeInMillis()/1000L); 
	}
	/**
	 * 获取当前时间戳
	 * @return
	 */
	public static int getNowTime(){
		Calendar calendar = Calendar.getInstance(); 
		calendar.set(Calendar.MILLISECOND, 0); 
		return (int)(calendar.getTimeInMillis()/1000L); 
	}
	/**
	 * 获取当前时间N个小时之前的时间戳
	 * @return
	 */
	public static int getBeforeTime(int before){
		Calendar calendar = Calendar.getInstance(); 
		
		calendar.add(Calendar.HOUR_OF_DAY, before); 
		calendar.set(Calendar.MILLISECOND, 0); 
		return (int)(calendar.getTimeInMillis()/1000L); 
	}
	/**
	 * 获取前一天的开始时间戳
	 * @return
	 */
	public static int getLastDayBegin(){
		Calendar calendar = Calendar.getInstance(); 
		calendar.add(Calendar.DAY_OF_MONTH, -1);
		calendar.set(Calendar.HOUR_OF_DAY, 0); 
		calendar.set(Calendar.SECOND, 0); 
		calendar.set(Calendar.MINUTE, 0); 
		calendar.set(Calendar.MILLISECOND, 0); 
		return (int)(calendar.getTimeInMillis()/1000L); 
	}
	/**
	 * 获取前一天的结束时间戳
	 * @return
	 */
	public static int getLastDayEnd(){
		return getDayBeginTime();
		
	}
	/**
	 * 获取每周的第一个时间戳
	 * 星期日为一周的第一天	   SUN	MON	TUE	WED	THU	FRI	SAT
	 * DAY_OF_WEEK返回值	1	2	3	4	5	6	7
	 * @return
	 */
	public static int getWeekBeginTime(){
	    Calendar calendar = Calendar.getInstance();// 获取当前日期  
	    calendar.setFirstDayOfWeek(Calendar.MONDAY);
	    calendar.add(Calendar.MONTH, 0);  
	    calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);// 设置为1号,当前日期既为本月第一天  
	    calendar.set(Calendar.HOUR_OF_DAY, 0);  
	    calendar.set(Calendar.MINUTE, 0);  
	    calendar.set(Calendar.SECOND, 0); 
	    return (int)(calendar.getTimeInMillis()/1000L);  
	}
	/**
	 * 获取当前周的最后一个时间戳，其实是下一周的一个时间戳，不需要包括即可
	 * @return
	 */
	public static int getWeekEndTime(){
	    Calendar calendar = Calendar.getInstance();// 获取当前日期  
	    calendar.setFirstDayOfWeek(Calendar.MONDAY);
	    calendar.add(Calendar.WEEK_OF_MONTH, 1);  
	    calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);// 设置为1号,当前日期既为本月第一天  
	    calendar.set(Calendar.HOUR_OF_DAY, 0);  
	    calendar.set(Calendar.MINUTE, 0);  
	    calendar.set(Calendar.SECOND, 0); 
	    return (int)(calendar.getTimeInMillis()/1000L);
	}
	/**
	 * 获取当月的第一个时间戳
	 * @return
	 */
	public static int getMonthBeginTime() {  
	    Calendar calendar = Calendar.getInstance();// 获取当前日期  
	    calendar.add(Calendar.MONTH, 0);  
	    calendar.set(Calendar.DAY_OF_MONTH, 1);// 设置为1号,当前日期既为本月第一天  
	    calendar.set(Calendar.HOUR_OF_DAY, 0);  
	    calendar.set(Calendar.MINUTE, 0);  
	    calendar.set(Calendar.SECOND, 0);    
	    return (int)(calendar.getTimeInMillis()/1000L);  
	}  
	/**
	 * 获取当月的最后一个时间戳
	 * @return
	 */
	public static int getMonthEndTime() {  
	    Calendar calendar = Calendar.getInstance();// 获取当前日期  
	    calendar.add(Calendar.MONTH, 1);  
	    calendar.set(Calendar.DAY_OF_MONTH, 1);// 设置为1号,当前日期既为本月第一天  
	    calendar.set(Calendar.HOUR_OF_DAY, 0);  
	    calendar.set(Calendar.MINUTE, 0);  
	    calendar.set(Calendar.SECOND, 0);    
	    return (int)(calendar.getTimeInMillis()/1000L);  
	} 
	
	
	/** 
	* 获得指定日期的前n天 
	* @param specifiedDay 
	* @return 
	* @throws Exception 
	*/ 
	public static String getSpecifiedDayBeforeNDay(String specifiedDay,int n) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyyMMdd").parse(specifiedDay);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		c.setTime(date);
		int day = c.get(Calendar.DATE);
		c.set(Calendar.DATE, day - n);
		String dayBefore = new SimpleDateFormat("yyyyMMdd").format(c.getTime());
		return dayBefore;
	}
	
	/** 
	* 获得指定日期的前n月 
	* @param specifiedDay 
	* @return 
	* @throws Exception 
	*/ 
	public static String getSpecifiedDayBeforeNMon(String specifiedDay,int n) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyyMMdd").parse(specifiedDay);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		c.setTime(date);
		int mon = c.get(Calendar.MONTH);
		c.set(Calendar.MONTH,mon - n);
		c.set(Calendar.DAY_OF_MONTH, 1);
		String month = new SimpleDateFormat("yyyyMMdd").format(c.getTime());
		return month;
	}
	
	/** 
	* 获得指定日期的前n周
	* @param specifiedDay 
	* @return 
	* @throws Exception 
	*/ 
	public static String getSpecifiedDayBeforeNWeek(String specifiedDay,int n) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyyMMdd").parse(specifiedDay);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		c.setTime(date);
		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.add(Calendar.WEEK_OF_YEAR, -n);
		c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
	   	c.add(Calendar.MONTH, 0);  
	    c.set(Calendar.HOUR_OF_DAY, 0);  
	    c.set(Calendar.MINUTE, 0);  
	    c.set(Calendar.SECOND, 0); 
		String week = new SimpleDateFormat("yyyyMMdd").format(c.getTime());
		return week;
	}
	
	/** 
	* 获得指定日期的前n周
	* @param specifiedDay 
	* @return 
	* @throws Exception 
	*/ 
	public static String getSpecifiedDayBeforeNSeason(String specifiedDay,int n) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyyMMdd").parse(specifiedDay);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		c.setTime(date);
		int mon = c.get(Calendar.MONTH);
		int season = getSeason(mon);
		c.set(Calendar.MONTH,season - n*3);
		c.set(Calendar.DAY_OF_MONTH, 1);
		String month = new SimpleDateFormat("yyyyMMdd").format(c.getTime());
		return month;
	}
	
	
	public static int getSeason(int month) {  
		  
        int season = 0;  
        switch (month) {  
        case Calendar.JANUARY:  
        case Calendar.FEBRUARY:  
        case Calendar.MARCH:  
            season = Calendar.JANUARY;  
            break;  
        case Calendar.APRIL:  
        case Calendar.MAY:  
        case Calendar.JUNE:  
            season = Calendar.APRIL;  
            break;  
        case Calendar.JULY:  
        case Calendar.AUGUST:  
        case Calendar.SEPTEMBER:  
            season = Calendar.JULY;  
            break;  
        case Calendar.OCTOBER:  
        case Calendar.NOVEMBER:  
        case Calendar.DECEMBER:  
            season = Calendar.OCTOBER;
            break;  
        default:  
            break;  
        }  
        return season;  
    }  
	public static List<Map.Entry<String, Long>> sortAndTopN(Map<String,Long> lists ,int topN){
		List<Map.Entry<String, Long>> rets_tmp = new ArrayList<Map.Entry<String, Long>>(lists.entrySet());
		Collections.sort(rets_tmp, new Comparator<Map.Entry<String, Long>>() {   
		    public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {      
		    	return  (int) (o2.getValue() - o1.getValue()); 
		    }
		}); 
		if(rets_tmp.size() < topN){
			return rets_tmp;
		}
		return rets_tmp.subList(0, topN);
	}
	/**
	 * 简单的json排序
	 * @param obj
	 * @return
	 */
    public static JSONArray sortJsonObject(JSONObject obj) {  
        Map map = new TreeMap();  
        Iterator<String> it = obj.keys();  
        while (it.hasNext()) {  
            String key = it.next();  
            long value = Long.parseLong(obj.get(key).toString());   
            map.put( key,value);  
 
        }  
        List<Map.Entry<String, Long>> tmps = sortAndTopN(map,100);
        List<Map<String,String>> ret = new ArrayList<Map<String,String>>();
        Map<String,String> item = null;
        int index = 0;
        for(Map.Entry<String, Long> tmp:tmps){
        	item = new HashMap<String,String>();
        	item.put("name", tmp.getKey());
        	item.put("value",tmp.getValue().toString());
        	ret.add(index, item);
        	index++;
        }
        Jlog.info(ret.toString());
        return JSONArray.fromObject(ret);  
    }
    /**
     * 把JSONArray的数组，转换成 JSONArray里面都是key value相同的对象。
     * @param lists
     * @return
     */
    public static JSONArray formatJsonArrayToPairArray(JSONArray lists){
    	Iterator ite = lists.iterator();
    	JSONArray rets = new JSONArray();
    	JSONObject ret = new JSONObject();
    	ret.put("所有","0");
    	rets.add(0,ret);
    	int i = 1;
    	while(ite.hasNext()){
    		String item = ite.next().toString();
    		if(item.isEmpty()){
    			continue;
    		}
    		ret = new JSONObject();
    		ret.put(item, item);
    		rets.add(i,ret);
    		i++;
    	}
    	return rets;
    }
}
