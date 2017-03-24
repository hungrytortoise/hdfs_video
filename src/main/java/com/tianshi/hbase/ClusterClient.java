package com.tianshi.hbase;

import com.tianshi.tools.Jencode;
import com.tianshi.tools.Jlog;
import com.tianshi.tools.Jtools;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

@Component("clusterClient")
public class ClusterClient {
	private HbaseClient hcli = null;
	
	public ClusterClient(){
		hcli = new HbaseClient();
	}
	/**
	 * 获取日志概况
	 * @param ct d/w/m/a
	 * @return
	 */
	public JSONObject getCommon(String ct){
		JSONObject common  = new JSONObject();		
		JSONObject tmp  = new JSONObject();
		String day = null;
		String rowKey = null;
		String date = Jtools.getSpecifiedDayBeforeNDay(Jtools.getStrTime(Jtools.getDayBeginTime()+"", "yyyyMMdd"),2);
		if("d".equals(ct)){
			day = date;					//昨天
		}else if("w".equals(ct)){
			day = Jtools.getSpecifiedDayBeforeNWeek(date,0);//本周
		}else if("m".equals(ct)){
			day = Jtools.getSpecifiedDayBeforeNMon(date,0);//本月
		}else{
			return getAllCluster();
		}
		rowKey = HbaseConf.APP_PREFIX + ct + day;
		tmp = (JSONObject)this.get(rowKey,day).get(rowKey);
		tmp.put("name", HbaseConf.APP_PREFIX_NAME);
		common.put(HbaseConf.APP_PREFIX , tmp);
		
		rowKey = HbaseConf.SOC_PREFIX + ct + day;
		tmp = (JSONObject)this.get(rowKey,day).get(rowKey);
		tmp.put("name", HbaseConf.SOC_PREFIX_NAME);
		common.put(HbaseConf.SOC_PREFIX , tmp);
		
		rowKey = HbaseConf.PHONE_PREFIX + ct + day;
		tmp = (JSONObject)this.get(rowKey,day).get(rowKey);
		tmp.put("name", HbaseConf.PHONE_PREFIX_NAME);
		common.put(HbaseConf.PHONE_PREFIX ,tmp);
		
		rowKey = HbaseConf.WIFI_PREFIX + ct + day;
		tmp = (JSONObject)this.get(rowKey,day).get(rowKey);
		tmp.put("name", HbaseConf.WIFI_PREFIX_NAME);
		common.put(HbaseConf.WIFI_PREFIX , tmp);
		
		rowKey = HbaseConf.BS_PREFIX + ct + day;
		tmp = (JSONObject)this.get(rowKey,day).get(rowKey);
		tmp.put("name", HbaseConf.BS_PREFIX_NAME);
		common.put(HbaseConf.BS_PREFIX , tmp);
		
		rowKey = HbaseConf.TOTAL_PREFIX + ct + "_" + day;
		tmp = (JSONObject)this.get(rowKey,day).get(rowKey);
		tmp.put("name", HbaseConf.TOTAL_PREFIX_NAME);
		common.put("log_total", tmp);

		return common;
	}
	

	
	public JSONObject getAllCluster(){
		JSONObject obj = new JSONObject();
		JSONObject tmp  = null;
		//String key = HbaseConf.DEV_CLUSTER+"_all";
		//obj.put(HbaseConf.getRowKey(key),this.getValue(key).get(HbaseConf.getRowKey(key)));
		String key = HbaseConf.APP_CLUSTER+"_all";
		tmp = (JSONObject)this.getValue(key).get(HbaseConf.getRowKey(key));
		tmp.put("name", HbaseConf.APP_PREFIX_NAME);
		obj.put(HbaseConf.getRowKey(key),tmp);
		
		key = HbaseConf.SOC_CLUSTER+"_all";
		tmp = (JSONObject)this.getValue(key).get(HbaseConf.getRowKey(key));
		tmp.put("name", HbaseConf.SOC_PREFIX_NAME);
		obj.put(HbaseConf.getRowKey(key),tmp);
		
		key = HbaseConf.PHONE_CLUSTER+"_all";
		tmp = (JSONObject)this.getValue(key).get(HbaseConf.getRowKey(key));
		tmp.put("name", HbaseConf.PHONE_PREFIX_NAME);
		obj.put(HbaseConf.getRowKey(key),tmp);
		
		key = HbaseConf.WIFI_CLUSTER+"_all";
		tmp = (JSONObject)this.getValue(key).get(HbaseConf.getRowKey(key));
		tmp.put("name", HbaseConf.WIFI_PREFIX_NAME);
		obj.put(HbaseConf.getRowKey(key),tmp);
		
		key = HbaseConf.BS_CLUSTER+"_all";
		tmp = (JSONObject)this.getValue(key).get(HbaseConf.getRowKey(key));
		tmp.put("name", HbaseConf.BS_PREFIX_NAME);
		obj.put(HbaseConf.getRowKey(key),tmp);
		
		key = HbaseConf.TOTAL_CLUSTER+"_all";
		tmp = (JSONObject)this.getValue(key).get(HbaseConf.getRowKey(key));
		tmp.put("name", HbaseConf.TOTAL_PREFIX_NAME);
		obj.put(HbaseConf.getRowKey(key),tmp);
		
		/*
		String date = Jtools.getStrTime(Jtools.getDayBeginTime()+"", "yyyyMMdd");
		String day = Jtools.getSpecifiedDayBeforeNDay(date,2);//t-2
		String rowKey = HbaseConf.TOTAL_PREFIX + day;
		obj.put(rowKey, this.get(rowKey).get(rowKey));
		
		day = Jtools.getSpecifiedDayBeforeNDay(date,3);//t-3
		rowKey = HbaseConf.TOTAL_PREFIX + day;
		obj.put(rowKey, this.get(rowKey).get(rowKey));
		*/
		return obj;
	}
	
	
	public JSONObject getDEVCluster(){
		JSONObject obj = new JSONObject();
		String key = HbaseConf.DEV_CLUSTER+"_all";
		obj.put(HbaseConf.getRowKey(key),this.getValue(key).get(HbaseConf.getRowKey(key)));
		return obj;
	}
	
	
	
	public JSONObject getAPPCluster(){
		JSONObject obj = new JSONObject();
		String key = HbaseConf.APP_CLUSTER+"_all";
		obj.put(HbaseConf.getRowKey(key),this.getValue(key).get(HbaseConf.getRowKey(key)));
		return obj;
	}
	
	
	public JSONObject getSOCCluster(){
		JSONObject obj = new JSONObject();
		String key = HbaseConf.SOC_CLUSTER+"_all";
		obj.put(HbaseConf.getRowKey(key),this.getValue(key).get(HbaseConf.getRowKey(key)));
		return obj;
	}
	
	public JSONObject getPHONECluster(){
		JSONObject obj = new JSONObject();
		String key = HbaseConf.PHONE_CLUSTER+"_all";
		obj.put(HbaseConf.getRowKey(key),this.getValue(key).get(HbaseConf.getRowKey(key)));
		return obj;
	}
	
	public JSONObject getWIFICluster(){
		JSONObject obj = new JSONObject();
		String key = HbaseConf.WIFI_CLUSTER+"_all";
		obj.put(HbaseConf.getRowKey(key),this.getValue(key).get(HbaseConf.getRowKey(key)));
		return obj;
	}
	
	public JSONObject getBSCluster(){
		JSONObject obj = new JSONObject();
		String key = HbaseConf.BS_CLUSTER+"_all";
		obj.put(HbaseConf.getRowKey(key),this.getValue(key).get(HbaseConf.getRowKey(key)));
		return obj;
	}
	
	
	
	public JSONObject getClusterById(String id){
		Jlog.info("clusterid:"+id);
		JSONObject cluster = new JSONObject();
		if((HbaseConf.All_CLUSTER+"").equals(id)){
			cluster = this.getAllCluster();
		}
		if((HbaseConf.DEV_CLUSTER+"").equals(id)){
			cluster = this.getDEVCluster();
		}
		if((HbaseConf.APP_CLUSTER+"").equals(id)){
			cluster = this.getAPPCluster();
		}
		if((HbaseConf.SOC_CLUSTER+"").equals(id)){
			cluster = this.getSOCCluster();
		}
		if((HbaseConf.PHONE_CLUSTER+"").equals(id)){
			cluster = this.getPHONECluster();
		}
		if((HbaseConf.WIFI_CLUSTER+"").equals(id)){
			cluster = this.getWIFICluster();
		}
		if((HbaseConf.BS_CLUSTER+"").equals(id)){
			cluster = this.getBSCluster();
		}
		return cluster;
	}
	
	public JSONObject getClusterById(String id,String day){
		Jlog.info("clusterid:"+id);
		JSONObject cluster = new JSONObject();
		if((HbaseConf.All_CLUSTER+"").equals(id)){
			cluster = this.getAllCluster();
		}
		if((HbaseConf.DEV_CLUSTER+"").equals(id)){
			cluster = this.getDEVCluster();
		}
		if((HbaseConf.APP_CLUSTER+"").equals(id)){
			cluster = this.getAPPCluster();
		}
		if((HbaseConf.SOC_CLUSTER+"").equals(id)){
			cluster = this.getSOCCluster();
		}
		if((HbaseConf.PHONE_CLUSTER+"").equals(id)){
			cluster = this.getPHONECluster();
		}
		if((HbaseConf.WIFI_CLUSTER+"").equals(id)){
			cluster = this.getWIFICluster();
		}
		if((HbaseConf.BS_CLUSTER+"").equals(id)){
			cluster = this.getBSCluster();
		}
		return cluster;
	}
	
	public JSONObject getValue(String key){
		Jlog.info("getValue key:"+key);
		String rowkey = HbaseConf.getRowKey(key);
		return this.get(rowkey);
	}
	
	public JSONObject getValue(String key,String day){
		Jlog.info("getValue key:"+key + ",day:"+day);
		//key = key + "_" + day;
		String rowkey = (String) HbaseConf.getRowKeyByDay(day).get(key);
		return this.get(rowkey);
	}
	
	public JSONObject get(String rowkey){
		JSONObject obj = new JSONObject();
		JSONObject value = null;
		try {
			value = this.hcli.getValue(HbaseConf.TABLE, rowkey, HbaseConf.COLUMFAMILY);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		obj.put(rowkey, value);
		return obj;
	}
	
	public JSONObject get(String rowkey,String day){
		JSONObject obj = new JSONObject();
		JSONObject value = null;
		try {
			value = this.hcli.getValue(HbaseConf.TABLE, rowkey, HbaseConf.COLUMFAMILY);
		} catch (IOException e) {
			Jlog.error("get meet excption:"+e.getMessage());
		}
		value.put("date", day);
		obj.put(rowkey, value);
		return obj;
	}
	
	public JSONObject getLR(String rowkey){
		JSONObject obj = new JSONObject();
		String value = null;
		try {
			value = this.hcli.getValue(HbaseConf.TABLE, rowkey, HbaseConf.COLUMFAMILY,  HbaseConf.COL01);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		obj.put(rowkey, value);
		return obj;
	}
	
	public JSONObject getSize(String rowkey){
		JSONObject obj = new JSONObject();
		String value = null;
		try {
			value = this.hcli.getValue(HbaseConf.TABLE, rowkey, HbaseConf.COLUMFAMILY,  HbaseConf.COL01);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		obj.put(rowkey, value);
		return obj;
	}
	
	public JSONObject getClusterByDay(String clusterId,String day){
		JSONObject obj = new JSONObject();
		if("0".equals(clusterId)){
			String key = HbaseConf.DEV_CLUSTER + "_" + day;
			String rowkey = (String) HbaseConf.getRowKeyByDay(day).get(key);
			obj.put(rowkey,this.getValue(key,day).get(rowkey));
			
			key = HbaseConf.APP_CLUSTER + "_" + day;
		    rowkey = (String) HbaseConf.getRowKeyByDay(day).get(key);
		    obj.put(rowkey,this.getValue(key,day).get(rowkey));
			
			key = HbaseConf.SOC_CLUSTER + "_" + day;
		    rowkey = (String) HbaseConf.getRowKeyByDay(day).get(key);
		    obj.put(rowkey,this.getValue(key,day).get(rowkey));
			
			key = HbaseConf.PHONE_CLUSTER + "_" + day;
		    rowkey = (String) HbaseConf.getRowKeyByDay(day).get(key);
		    obj.put(rowkey,this.getValue(key,day).get(rowkey));
			
			key = HbaseConf.WIFI_CLUSTER + "_" + day;
		    rowkey = (String) HbaseConf.getRowKeyByDay(day).get(key);
		    obj.put(rowkey,this.getValue(key,day).get(rowkey));
			
			key = HbaseConf.BS_CLUSTER + "_" + day;
		    rowkey = (String) HbaseConf.getRowKeyByDay(day).get(key);
		    obj.put(rowkey,this.getValue(key,day).get(rowkey));
		}else{
			String key = clusterId + "_" + day;
			Jlog.info("getClusterByDay,key: "+key);
			String rowkey = (String) HbaseConf.getRowKeyByDay(day).get(key);
			Jlog.info("getClusterByDay,rowkey: "+rowkey);
			obj.put(rowkey,this.getValue(key,day).get(rowkey));
		}
		return obj;
	}


	public JSONArray getClusterByKey(String key, String daystart, String dayend) {
		String startRowkey = key + "_" + daystart;
		String endRowkey = key + "_" + dayend;
		JSONArray cells = null;
		try {
		     cells = this.hcli.scanData(HbaseConf.TABLE, startRowkey, endRowkey);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO Auto-generated method stub
		return cells;
	}
	
	public JSONObject getListData(String ct,String key,int limit){
		String prefix = null;
		if("log_total".equals(key)){
			prefix = key + "_" + ct + "_";
		}else{
			prefix = key + ct;
		}
		List<String> rowkeys = HbaseConf.getCTKey(ct, limit, null,prefix,"");
		JSONObject list = null;
		try {
			list = this.hcli.getListValue(HbaseConf.TABLE, rowkeys, HbaseConf.COLUMFAMILY,prefix);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	
	public JSONObject getListData(String ct,List<String> key,int limit){
		List<String> rowkeys = HbaseConf.getCTKey(ct, limit, null,key,"");
		Jlog.info("rowkeys list:"+rowkeys.toString());
		JSONObject list = null;
		try {
			list = this.hcli.getListValue(HbaseConf.TABLE, rowkeys, HbaseConf.COLUMFAMILY,ct);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	
	/**
	 * sr[o/s/a/d/u][d/w/m]20160809p0_r0		全部平台/全部国家
	 * @param ct
	 * @param limit
	 * @param plat
	 * @param
	 * @param
	 * * @return
	 */
	public JSONObject getTrendListData(String ct, String plat, String key, String value, int limit){
		List<String> srsList =new ArrayList<String>() ;
		List<String> sraList =new ArrayList<String>() ;
		List<String> srdList =new ArrayList<String>() ;
		List<String> sruList =new ArrayList<String>() ;
		//o 
		String prefix ="sro"+ct;
		String endfix="p"+plat+"_"+key+value;
		List<String> rowkeys=HbaseConf.getCTKey(ct, limit, null, prefix, endfix) ;		
		//s
		prefix="srs"+ct;
		srsList=HbaseConf.getCTKey(ct, limit, null, prefix, endfix);
		rowkeys.addAll(srsList);
		//a
		prefix="sra"+ct;
		sraList=HbaseConf.getCTKey(ct, limit, null, prefix, endfix);
		rowkeys.addAll(sraList);
		//d
		prefix="srd"+ct;
		srdList=HbaseConf.getCTKey(ct, limit, null, prefix, endfix);
		rowkeys.addAll(srdList);
		//u
		prefix="sru"+ct;
		sruList=HbaseConf.getCTKey(ct, limit, null, prefix, endfix);
		rowkeys.addAll(sruList);
		
		Jlog.info("getTrendListData rowkeys:"+rowkeys.toString());
		JSONObject list = null;
		try {
			
			list=this.hcli.getListValue(HbaseConf.TABLE, rowkeys, HbaseConf.COLUMFAMILY,prefix);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		//list--》  json{“rowkey1”：{value1}，“rowkey2”：{value2}}
		return list;
	}
	
	
	public JSONObject getTrendListData(String ct, int limit,String prefix,String kind,String endfix){
		//拼接规则    st+kind+ct+时间   soc_total_oauth_all	授权帐号累计总量(日志条数总量，不去重)
		prefix= prefix+kind+ct;
		List<String> rowkeys=null;
		if(endfix ==null){
			//时间设置null 取当前时间的前两天
			rowkeys= HbaseConf.getCTKey(ct, limit, null,prefix,null);
			Jlog.info("getTrendListData rowkeys:"+rowkeys.toString());
		}
		JSONObject list = null;
		try {
			list=this.hcli.getListValue(HbaseConf.TABLE, rowkeys, HbaseConf.COLUMFAMILY,prefix);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return list;
	}
	public JSONObject getAllTrends(String ct, int limit){
		JSONObject trends = new JSONObject();
		String key = HbaseConf.APP_PREFIX;
		JSONObject report = this.getListData(ct,key,limit);
		trends.put(key, report.values());
		
		key = HbaseConf.SOC_PREFIX;
		report = this.getListData(ct,key,limit);
		trends.put(key, report.values());
		
		key = HbaseConf.PHONE_PREFIX;
		report = this.getListData(ct,key,limit);
		trends.put(key, report.values());
		
		key = HbaseConf.WIFI_PREFIX;
		report = this.getListData(ct,key,limit);
		trends.put(key, report.values());
		
		key = HbaseConf.BS_PREFIX;
		report = this.getListData(ct,key,limit);
		trends.put(key, report.values());
		
		
		
		return trends;
	}

	@SuppressWarnings({ "unused", "unchecked"})
	public JSONObject getTableData(String ct, String cluster) {
		JSONObject tableData = new JSONObject();
		if(HbaseConf.getTableByCluster().containsKey(cluster)){
			List<String> tables = (List<String>) HbaseConf.getTableByCluster().get(cluster);
			Jlog.info("table list:"+tables.toString());
			tableData = this.getListData(ct, tables, 1);
		}else{
			Jlog.info("cluster not exist:"+cluster);
		}
		return tableData;
	}

	public JSONObject getData(String rowKey) {
		JSONObject list = null;
		try {
			list = this.hcli.getTotalValue(HbaseConf.TABLE, rowKey, HbaseConf.COLUMFAMILY);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}

	public JSONObject getTrendListData(String ct, int limit, String country, String province, String city, String key,
			String value) {
		
		String prefix = "drp"+ct;
		String endfix = "r"+country+"_"+province+"_"+city+"_"+key+value;
		
		List<String> rowkeys = HbaseConf.getCTKey(ct, limit, null,prefix,endfix);
		Jlog.info("getTrendListData rowkeys:"+rowkeys.toString());
		JSONObject list = null;
		try {
			list = this.hcli.getListValue(HbaseConf.TABLE, rowkeys, HbaseConf.COLUMFAMILY,prefix);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	
	public JSONObject getAppTrendListData(String ct, int limit, String plat,String version,String app) {
		String prefix = "";
		String endfix = "";
		if("list".equals(app)){
			prefix = "app_total_list_all_"+ct+"_";
		}else if("active".equals(app)){
			prefix = "app_total_active_all_"+ct+"_";
		}else{
			prefix = "app_total_sdk_all_"+ct+"_";
			endfix = "_"+plat+"_"+version;
		}
		List<String> rowkeys = HbaseConf.getCTKey(ct, limit, null,prefix,endfix);
		Jlog.info("getAppTrendListData rowkeys:"+rowkeys.toString());
		JSONObject list = null;
		try {
			list = this.hcli.getListValue(HbaseConf.TABLE, rowkeys, HbaseConf.COLUMFAMILY,prefix);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	/**
	 *  app 排行榜
	 * @param ct 周期 d 天/w 周/m 月
	 * @param limit 获取几条结果
	 * @param type 动作类型：install 安装列表，uninstall 卸载，new_install 新安装
	 * @param app 模块信息： list 监控的app，active 活跃的，sdk sdk 覆盖的。
	 * @return
	 */
	public JSONArray getAppTopTrendListData(String ct, int limit, String type,String app) {
		String prefix = "";
		//String endfix = "_"+plat;
		String endfix = "";
		if("list".equals(app)){
			prefix = "app_top_list_"+ct+"_";
		}else if("active".equals(app)){
			prefix = "app_top_active_"+ct+"_";
		}else{
			prefix = "app_top_sdk_"+ct+"_";
		}
		List<String> rowkeys = HbaseConf.getCTKey(ct, limit, null,prefix,endfix);
		if("d".equals(ct)){
			rowkeys.remove(0);//列表前天
		}else if("m".equals(ct) ||"w".equals(ct)  ){
			rowkeys.remove(1);//上一周或者上一个月
		}
		Jlog.info("getAppTopTrendListData rowkeys:"+rowkeys.toString());
		JSONObject list = null;
		try {
			list = this.hcli.getListValue(HbaseConf.TABLE, rowkeys, HbaseConf.COLUMFAMILY,prefix);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		JSONArray rank = new JSONArray();
		if(list.get(rowkeys.get(0)) != null){
			JSONObject listinfo = (JSONObject) list.get(rowkeys.get(0));
			Jlog.info("top list0:"+list.toString());
			
			//中文hbase有乱码，先base64编码存储，然后取的时候再解码
			listinfo = JSONObject.fromObject(Jencode.getFromBase64(listinfo.get("list").toString()));
			//listinfo = JSONObject.fromObject(listinfo.get("list"));
			Jlog.info("listinfo:"+listinfo.toString()); 
			
			//if( listinfo.get(type) != null){
			//	rank = JSONArray.fromObject(listinfo.get(type));
			//}
			JSONArray rank_tmp = (JSONArray) listinfo.get(type);
			Iterator<Object> it = rank_tmp.iterator();
			while(it.hasNext()){
				JSONObject ob = (JSONObject) it.next();
				if(ob.containsKey("summary")){		//summary有双引号，如果显示 ，则需要替换。
					//ob.put("summary",ob.get("summary").toString().replaceAll("\"", "“"));
					ob.put("summary","");
				}
				rank.add(ob);
			}
		}
		return rank;
	}
	/**
	 * 趋势图表的schema
	 * @return
	 */
	public JSONObject getTrendSchema(){
		JSONObject tmp  = new JSONObject();
		tmp.put(HbaseConf.APP_PREFIX, HbaseConf.APP_PREFIX_NAME);
		tmp.put(HbaseConf.SOC_PREFIX, HbaseConf.SOC_PREFIX_NAME);
		tmp.put(HbaseConf.PHONE_PREFIX, HbaseConf.PHONE_PREFIX_NAME);
		tmp.put(HbaseConf.WIFI_PREFIX, HbaseConf.WIFI_PREFIX_NAME);
		tmp.put(HbaseConf.BS_PREFIX, HbaseConf.BS_PREFIX_NAME);
		return tmp;
	}
	/**
	 * 日志表的schema
	 * @return
	 */
	public JSONObject getLogTableSchema(){
		JSONObject tmp  = new JSONObject();
		tmp.put("num","数据条数");
		tmp.put("size","数据大小");
		tmp.put("table","数据表");
		return tmp;
	}
	/**
	 * App排行榜schema
	 * @return
	 */
	public JSONArray getAppTopSchema(){
		JSONArray list = new JSONArray();
		JSONObject tmp  = new JSONObject();
		tmp.put("key", "index");
		tmp.put("name", "排名");
		list.add(0,tmp);
		tmp  = new JSONObject();
		tmp.put("key", "logo");
		tmp.put("name", "Logo");
		list.add(1,tmp);
		tmp  = new JSONObject();
		tmp.put("key", "app_name");
		tmp.put("name", "应用");
		list.add(2,tmp);
		tmp  = new JSONObject();
		tmp.put("key", "artist");
		tmp.put("name", "作者");
		list.add(3,tmp);
		tmp  = new JSONObject();
		tmp.put("key", "rank_change");
		tmp.put("name", "排名变化");
		list.add(4,tmp);


		/*tmp.put("index","排名");
		tmp.put("app_name","应用");
		tmp.put("rank_change","排名变化");
		tmp.put("artist","作者");
		//tmp.put("summary","简介");
		tmp.put("logo","Logo");*/
		return list;
	}
	/**
	 * 组合设备分布的key
	 * @param dimension   plat(默认)/global/china/os/factory/model/screensize/net/carrier
	 * @param day
	 * @param plat
	 * @return
	 */
	public String makeDevTopKey(String dimension,String day,String plat){
		String key = "";
		Map<String,String> _conf = new HashMap<String,String>(){{
			put("plat","dev_top_plat_m_");
			put("global","dev_top_global_m_");
			put("china","dev_top_china_m_");
			put("os","dev_top_os_m_");
			put("factory","dev_top_factory_m_");
			put("model","dev_top_model_m_");
			put("screensize","dev_top_screensize_m_");
			put("net","dev_top_net_m_");
			put("carrier","dev_top_carrier_m_");
		}
		};
		if(dimension.equals("plat") || dimension.equals("os") || dimension.equals("factory")|| dimension.equals("model")|| dimension.equals("screensize") 
				 || dimension.equals("net")  || dimension.equals("carrier") ){
			key = _conf.get(dimension)+day;
		}else if(dimension.equals("global") || dimension.equals("china")){
			if(plat.equals("0") || plat.equals("1") || plat.equals("2")){
				key = _conf.get(dimension)+day+"_"+plat;
			}else{
				key = _conf.get(dimension)+day+"_0";
			}
			
		}
		return key;
	}
	public void set(String rowkey,Object value,String col){
		try {
			 this.hcli.insterRow(HbaseConf.TABLE, rowkey, HbaseConf.COLUMFAMILY,col,String.valueOf(value));
		} catch (IOException e) {
			Jlog.error("set to hbase error:"+e.getMessage());
		}
	}
	
}
