/**
 * 
 */
package com.tianshi.hbase;


import com.tianshi.tools.Jlog;
import com.tianshi.tools.Jres;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class HbaseClient {

	public static Configuration configuration = null;
	public static Connection connection =null;
	public static Admin admin = null;

	public HbaseClient() {
		init();
	}
	protected  void finalize(){
		close();
	}

	// 初始化链接
	public void init() {
		if(HbaseClient.connection ==null || HbaseClient.connection.isClosed()){
			configuration = HBaseConfiguration.create();
			String oldconf = "10.3.40.8,10.2.96.29,10.3.129.9";
			//String newconf = "10.3.5.39,10.3.5.88,10.3.5.89";
			String newconf = "10.6.24.107,10.6.24.111,10.6.24.113";
			configuration.set("hbase.zookeeper.quorum", newconf);
			configuration.set("hbase.zookeeper.property.clientPort", "2181");
			configuration.set("zookeeper.znode.parent", "/hbase");
	
			try {
				connection = ConnectionFactory.createConnection(configuration);
				admin = connection.getAdmin();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// 关闭连接
	public void close() {
		try {
			if (null != admin)
				admin.close();
			if (null != connection)
				connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// 建表,如果表结构存在，则先要disable,然后再delete，再创建
	public void createTable(String tableNmae, String[] cols) throws IOException {

		TableName tableName = TableName.valueOf(tableNmae);
		
		if (admin.tableExists(tableName)) {
			Jlog.info("talbe is exists!");
		} else {
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
			for (String col : cols) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
				hTableDescriptor.addFamily(hColumnDescriptor);
			}
			admin.createTable(hTableDescriptor);
			Jlog.info("hbase create table:"+tableName);
		}

	}

	// 删表
	public void deleteTable(String tableName) throws IOException {

		TableName tn = TableName.valueOf(tableName);
		if (admin.tableExists(tn)) {
			admin.disableTable(tn);
			admin.deleteTable(tn);
		}
	}

	// 查看已有表
	public void listTables() throws IOException {
		HTableDescriptor hTableDescriptors[] = admin.listTables();
		for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
			System.out.println(hTableDescriptor.getNameAsString());
		}
	}

	// 插入数据
	public void insterRow(String tableName, String rowkey, String colFamily, String col, String val)
			throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowkey));
		put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
		table.put(put);

		// 批量插入
		/*
		 * List<Put> putList = new ArrayList<Put>(); puts.add(put);
		 * table.put(putList);
		 */
		table.close();
	}

	// 删除数据
	public void deleRow(String tableName, String rowkey, String colFamily, String col) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(Bytes.toBytes(rowkey));
		// 删除指定列族
		// delete.addFamily(Bytes.toBytes(colFamily));
		// 删除指定列
		delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
		table.delete(delete);
		// 批量删除
		/*
		 * List<Delete> deleteList = new ArrayList<Delete>();
		 * deleteList.add(delete); table.delete(deleteList);
		 */
		table.close();
	}

	// 根据rowkey查找数据
	public JSONArray getData(String tableName, String rowkey, String colFamily, String col) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowkey));
		// 获取指定列族数据
		// get.addFamily(Bytes.toBytes(colFamily));
		// 获取指定列数据
		 get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
		Result result = table.get(get);
		//showCell(result);
		table.close();
		return getCell(result);
	}
	
	public JSONArray getData(String tableName, String rowkey, String colFamily) throws IOException {
		init();
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowkey));
		// 获取指定列族数据
		 get.addFamily(Bytes.toBytes(colFamily));
		// 获取指定列数据
		// get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));

		 
		 //TODO 这边有问题
		Result result = table.get(get);
		//showCell(result);
		table.close();
		return getCell(result);
	}
	
	public String getValue(String tableName, String rowkey, String colFamily, String col) throws IOException{
		String value = null;
		Jlog.info("rowkey:"+rowkey);
		JSONArray cells = this.getData(tableName, rowkey, colFamily, col);
		if(cells.size() > 0){
			JSONObject cell = (JSONObject) cells.get(0);
			value = cell.getString("value");
			Jlog.info("getValue :"+cell.toString());
		}
		return value;
	}
	
	public JSONObject getValue(String tableName, String rowkey, String colFamily) throws IOException{
		JSONObject value = new JSONObject();
		if(value.isEmpty()){
			value.put("rowKey", rowkey);
			value.put("nums", 0);
			value.put("size", 0);
			value.put("lr", 0);
			value.put("size_lr", 0);
		}
		JSONArray cells = this.getData(tableName, rowkey, colFamily);
		Jlog.info(" get value rowkey:"+rowkey+ " cell:"+cells.toString());	
		for(int i = 0 ; i < cells.size() ; i++){
			JSONObject cell = (JSONObject) cells.get(i);
			value.put(cell.getString("column"), cell.getString("value"));
		}	
		return value;
	}
	
	public JSONObject getTotalValue(String tableName, String rowkey, String colFamily) throws Exception{
		JSONObject value =new JSONObject ();
		if(value.isEmpty()){
			value.put("rowkey", rowkey);
			value.put("nums",0);
		}
		JSONArray cells =this.getData(tableName, rowkey, colFamily);
		Jlog.info(" get value rowkey:"+rowkey+ " cell:"+cells.toString());
		for(int i=0;i<cells.size();i++){
			JSONObject cell = (JSONObject)cells.get(i) ;
			value.put(cell.getString ("column"), cell.getString("value")) ;
		}
		return value;
	}
	
	
	public JSONArray getListData(String tableName, List<String> rowkeyList, String colFamily) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		List<Get> getList = new ArrayList<Get>();
		for(String rowkey:rowkeyList){
			Get get = new Get(Bytes.toBytes(rowkey));
			//get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
			get.addFamily(Bytes.toBytes(colFamily));
			getList.add(get);
		}
	
		// 获取指定列族数据
		// get.addFamily(Bytes.toBytes(colFamily));
		// 获取指定列数据
		 
		Result[] result = table.get(getList);
		table.close();
		return getCell(result);
	}
	
	public JSONObject getListValue(String tableName, List<String> rowkeyList, String colFamily) throws IOException{
		JSONObject list = new JSONObject();
		JSONArray celllist = this.getListData(tableName, rowkeyList, colFamily);
		for(int i = 0 ; i < celllist.size() ; i++){
			JSONObject cell = (JSONObject) celllist.get(i);
			JSONObject value = new JSONObject();
			if(list.containsKey(cell.getString("rowKey"))){
				value = (JSONObject) list.get(cell.getString("rowKey"));
			}
			if("nums".equals(cell.getString("column"))){
				value.put("nums", cell.getString("value"));
			}
			if("size".equals(cell.getString("column"))){
				value.put("size", cell.getString("value"));
			}
			if("lr".equals(cell.getString("column"))){
				value.put("lr", cell.getString("value"));
			}
			if("size_lr".equals(cell.getString("column"))){
				value.put("size_lr", cell.getString("value"));
			}
			if(value.isEmpty()){
				value.put("nums", 0);
				value.put("size", 0);
				value.put("lr", 0);
				value.put("size_lr", 0);
			}else if(!value.containsKey("nums")){
				value.put("nums", 0);
				Jlog.info("debug....");
			}else if(!value.containsKey("size")){
				value.put("size", 0);
			}else if(!value.containsKey("lr")){
				value.put("lr", 0);
			}else if(!value.containsKey("size_lr")){
				value.put("size_lr", 0);
			}
			list.put(cell.getString("rowKey"), value);
			Jlog.info("getListValue cell:"+cell.toString() + ",value:"+value.toString());
		}
		
		return list;
	}
	
	public JSONObject getListValue(String tableName, List<String> rowkeyList, String colFamily,String ct) throws IOException{
		JSONObject list = new JSONObject();
		JSONArray celllist = this.getListData(tableName, rowkeyList, colFamily);
		String rowKey = null;
		for(int i = 0 ; i < celllist.size() ; i++){
			JSONObject cell = (JSONObject) celllist.get(i);
			JSONObject value = new JSONObject();
			rowKey = cell.getString("rowKey");
			String dayKey = Jres.getNumFromStr(rowKey);
			value.put("rowkey", rowKey);
			value.put("table", HbaseConf.getTableName().get(rowKey.replace(ct+dayKey, "")));
			value.put("date", dayKey);
			if(list.containsKey(rowKey)){
				value = (JSONObject) list.get(rowKey);
			}
			value.put(cell.getString("column"), cell.getString("value"));
			if(value.isEmpty()){
				value.put("nums", 0);
				value.put("size", 0);
				value.put("lr", 0);
				value.put("size_lr", 0);
			}else if(!value.containsKey("nums")){
				value.put("nums", 0);
			}else if(!value.containsKey("size")){
				value.put("size", 0);
			}else if(!value.containsKey("lr")){
				value.put("lr", 0);
			}else if(!value.containsKey("size_lr")){
				value.put("size_lr", 0);
			}
			list.put(rowKey, value);
		}
		Jlog.info("getListValue list:"+list.toString());
		return list;
	}

	// 格式化输出
	public void showCell(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
			System.out.println("Timetamp:" + cell.getTimestamp() + " ");
			System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
			System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
			System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
		}
	}
	
	
	
	public JSONArray getCell(Result result){
		JSONArray cellsArr = new JSONArray();
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			JSONObject cellObj = new JSONObject();
			cellObj.put("rowKey", new String(CellUtil.cloneRow(cell)));
			cellObj.put("columnFamily", new String(CellUtil.cloneFamily(cell)));
			cellObj.put("column", new String(CellUtil.cloneQualifier(cell)));
			cellObj.put("value", new String(CellUtil.cloneValue(cell)));
			cellObj.put("timetamp", cell.getTimestamp());
			cellObj.put("rowoffset", cell.getRowOffset());
			cellObj.put("valueoffset", cell.getValueOffset());
			cellsArr.add(cellObj);
		}
		return cellsArr;
	}
	
	public JSONArray getCell(Result[] results) throws UnsupportedEncodingException{
		JSONArray cellsArr = new JSONArray();
		for(Result result : results){
			Cell[] cells = result.rawCells();
			for (Cell cell : cells) {
				JSONObject cellObj = new JSONObject();
				cellObj.put("rowKey", new String(CellUtil.cloneRow(cell)));
				cellObj.put("columnFamily", new String(CellUtil.cloneFamily(cell)));
				cellObj.put("column", new String(CellUtil.cloneQualifier(cell)));
				cellObj.put("value", new String(CellUtil.cloneValue(cell)));
				cellObj.put("timetamp", cell.getTimestamp());
				cellObj.put("rowoffset", cell.getRowOffset());
				cellObj.put("valueoffset", cell.getValueOffset());
				Jlog.info("cellObj:"+cellObj.toString());
				cellsArr.add(cellObj);
			}
		}
		
		return cellsArr;
		
	}

	// 批量查找数据
	public JSONArray scanData(String tableName, String startRow, String stopRow) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startRow));
		scan.setStopRow(Bytes.toBytes(stopRow));
		ResultScanner resultScanner = table.getScanner(scan);
		JSONArray celllist = new JSONArray();
		Jlog.info("result length:");
		for (Result result : resultScanner) {
			JSONArray cell = getCell(result);
			if(cell.size() > 1){
				celllist.add(cell);
			}else{
				if(!cell.isEmpty()){
					celllist.add(cell.get(0));
				}
			}
		}
		table.close();
		return celllist;
	}
}
