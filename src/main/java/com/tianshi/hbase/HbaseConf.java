package com.tianshi.hbase;

import com.tianshi.tools.Jtools;
import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class HbaseConf {
	
	public static int All_CLUSTER = 0;
	public static int DEV_CLUSTER = 1;
	public static int APP_CLUSTER = 2;
	public static int SOC_CLUSTER = 4;
	public static int PHONE_CLUSTER = 8;
	public static int WIFI_CLUSTER = 16;
	public static int BS_CLUSTER = 32;
	public static int TOTAL_CLUSTER = 64;
	
	public static String DEV = "dev";
	public static String APP = "app";
	public static String SOC = "soc";
	public static String PHONE = "phone";
	public static String WIFI = "wifi";
	public static String BS = "bs";
	
	public static String TABLE = "dmp_stat_rep";
	public static String COLUMFAMILY = "data";
	public static String COL = "nums";
	public static String COL01 = "lr";
	public static String COL02 = "size";
	public static String COL03 = "size_lr";

	public static String APP_PREFIX = "lta";
	public static String APP_PREFIX_NAME = "APP日志";
	public static String SOC_PREFIX = "lts";
	public static String SOC_PREFIX_NAME = "社交日志";
	public static String PHONE_PREFIX = "ltp";
	public static String PHONE_PREFIX_NAME = "手机号日志";
	public static String WIFI_PREFIX = "ltw";
	public static String WIFI_PREFIX_NAME = "WIFI日志";
	public static String BS_PREFIX = "ltb";
	public static String BS_PREFIX_NAME = "基站日志";
	public static String DEV_PREFIX = "ltd";
	public static String TOTAL_PREFIX = "log_total_";
	public static String TOTAL_PREFIX_NAME = "总计";
	
	public static JSONObject getRowKey(){
		JSONObject rowKeys = new JSONObject();
		
		//rowKeys.put(DEV_CLUSTER+"_all", "log_total_dev_all");
		rowKeys.put(APP_CLUSTER+"_all", "log_total_app_all");
		rowKeys.put(SOC_CLUSTER+"_all", "log_total_soc_all");
		rowKeys.put(PHONE_CLUSTER+"_all", "log_total_phone_all");
		rowKeys.put(WIFI_CLUSTER+"_all", "log_total_wifi_all");
		rowKeys.put(BS_CLUSTER+"_all", "log_total_bs_all");
		rowKeys.put(TOTAL_CLUSTER+"_all", "log_total");
		
		return rowKeys;
	}
	
	public static JSONObject getRowKeyByDay(String day){
		JSONObject rowKeys = new JSONObject();
		//rowKeys.put(DEV_CLUSTER+"_"+day, "log_total_dev_"+day);
		rowKeys.put(APP_CLUSTER+"_"+day, "log_total_app_"+day);
		rowKeys.put(SOC_CLUSTER+"_"+day, "log_total_soc_"+day);
		rowKeys.put(PHONE_CLUSTER+"_"+day, "log_total_phone_"+day);
		rowKeys.put(WIFI_CLUSTER+"_"+day, "log_total_wifi_"+day);
		rowKeys.put(BS_CLUSTER+"_"+day, "log_total_bs_"+day);
		rowKeys.put(TOTAL_CLUSTER+"_"+day, "log_total_d_"+day);
		return rowKeys;
	}
	
	public static String getRowKey(String key){
		String rowkey = null;
		JSONObject rowKeys = HbaseConf.getRowKey();
		if(rowKeys.containsKey(key)){
			rowkey = rowKeys.getString(key);
		}
		return rowkey;
	}
	
	public static JSONObject getTableByCluster(){
		JSONObject cluster = new JSONObject();
		List<String> tables = new ArrayList<String>();
		
		tables.add("lrldiaai");
		tables.add("lrldiaii");
		tables.add("lrlduai");
		tables.add("lrlen");
		tables.add("lrlrn");
		
		cluster.put(APP, tables);
		tables.clear();
		
		tables.add("lrlsn");
		tables.add("lrlon");
		tables.add("lrlrn");

		cluster.put(SOC, tables);
		tables.clear();
		
		tables.add("lrbsi");
		cluster.put(BS, tables);
		
		tables.add("lrdei");
		cluster.put(WIFI, tables);
		tables.clear();
		
		return cluster;
	}
	
	public static JSONObject getTableName(){
		JSONObject table = new JSONObject();
		
		table.put("lrbsi", "dw_sdk_log.log_record_base_station_info");
		table.put("lrdei", "dw_sdk_log.log_record_device_ext_info");
		table.put("lrldi", "dw_sdk_log.log_record_log_device_info");
		
		table.put("lrldiaai", "dw_sdk_log.log_record_log_device_install_app_all_info");
		table.put("lrldiaii", "dw_sdk_log.log_record_log_device_install_app_incr_info");
		table.put("lrlduai", "dw_sdk_log.log_record_log_device_unstall_app_info");
		table.put("lrlen", "dw_sdk_log.log_record_log_event_new");
		table.put("lrlrn", "dw_sdk_log.log_record_log_run_new");
		
		table.put("lrlon", "dw_sdk_log.log_record_log_oauth_new");
		table.put("lrlsn", "dw_sdk_log.log_record_log_share_new");
		table.put("lrlan", "dw_sdk_log.log_record_log_snsapi_new");
		
		table.put("lrldiai", "dw_sdk_log.log_record_log_device_install_app_info");
		table.put("lrdar", "dw_sdk_log.log_record_device_app_runtimes");
		table.put("lrdei", "dw_sdk_log.log_record_device_ext_info");
		table.put("lrdei", "dw_sdk_log.log_record_device_ext_info");

		return table;
	}
	
	public static List<String> getCTKey(String ct,int limit,String date){
		return getCTKey(ct,limit,null,"","");
	}
	
	public static List<String> getCTKey(String ct,int limit,String date,String prefix,String endfix){
		List<String> keys = new ArrayList<String>();
		if(null == date){
			date = Jtools.getStrTime(Jtools.getDayBeginTime()+"", "yyyyMMdd");
		}
		String key = null;
		if("day".equals(ct) || "d".equals(ct)){
			for(int i = 0 ;i < limit ;i++){
				key = prefix + Jtools.getSpecifiedDayBeforeNDay(date,i+2) + endfix;
				keys.add(key);
			}
		}
		if("week".equals(ct) || "w".equals(ct)){
			for(int i = 0 ;i < limit ;i++){
				key = prefix + Jtools.getSpecifiedDayBeforeNWeek(date,i) + endfix;
				keys.add(key);
				
			}
		}
		if("month".equals(ct) || "m".equals(ct)){
			for(int i = 0 ;i < limit ;i++){
				key = prefix + Jtools.getSpecifiedDayBeforeNMon(date,i) + endfix;
				keys.add(key);
			}
		}
		if("season".equals(ct) || "q".equals(ct)){
			for(int i = 0 ;i < limit ;i++){
				key = prefix + Jtools.getSpecifiedDayBeforeNSeason(date,i) + endfix;
				keys.add(key);
			}
		}
		keys.sort(null);
		return keys;
	}
	
	public static List<String> getCTKey(String ct,int limit,String date,List<String> prefix,String endfix){
		List<String> keys = new ArrayList<String>();
		if(null == date){
			date = Jtools.getStrTime(Jtools.getDayBeginTime()+"", "yyyyMMdd");
		}
		String key = null;
		if("day".equals(ct) || "d".equals(ct)){
			for(int i = 0 ;i < limit ;i++){
				for(String pre:prefix){
					key = pre + ct + Jtools.getSpecifiedDayBeforeNDay(date,i) + endfix;
					keys.add(key);
				}
			}
		}
		if("week".equals(ct) || "w".equals(ct)){
			for(int i = 0 ;i < limit ;i++){
				for(String pre:prefix){
					key = pre + ct + Jtools.getSpecifiedDayBeforeNWeek(date,i) + endfix;
					keys.add(key);
				}
			}
		}
		if("month".equals(ct) || "m".equals(ct)){
			for(int i = 0 ;i < limit ;i++){
				for(String pre:prefix){
					key = pre + ct + Jtools.getSpecifiedDayBeforeNMon(date,i) + endfix;
					keys.add(key);
				}
			}
		}
		if("season".equals(ct) || "q".equals(ct)){
			for(int i = 0 ;i < limit ;i++){
				for(String pre:prefix){
					key = pre + ct + Jtools.getSpecifiedDayBeforeNSeason(date,i) + endfix;
					keys.add(key);
				}
			}
		}
		keys.sort(null);
		return keys;
	}
	
	
}
