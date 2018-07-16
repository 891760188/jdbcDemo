package ye.guo.huang.db;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import ye.guo.huang.tool.DbUtil;

public class CommonDBOperate {
	
	 private int pageSize = 0;
	 private int currentPage = 0;
	 private int totalCount = 0;
	 private int pageStart = 0;
	 private int pageEnd = 0;
 
	 private String sqlStar = " SELECT *  FROM (SELECT ROW_NUMBER() OVER(ORDER BY 1) AS RN, BUSINESS_QUERY.* FROM ( " ;
	 private String sqlEnd  = "    ) BUSINESS_QUERY) SUB_QUERY WHERE RN BETWEEN 1 AND 20 " ;
//	 private String sqlEnd  = "    ) BUSINESS_QUERY) SUB_QUERY WHERE RN BETWEEN " ;
	 

	 
	
	 public Map<String, Object>  getResultListOfPage2(String sql, List<Object> valuesList) throws SQLException {
		 	Connection connection = DbUtil.getConnection();
		 	connection.setAutoCommit(false);
			PreparedStatement preparedStatement = connection.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			for (int i = 0; i <valuesList.size(); i++) {
				Object value = valuesList.get(i);
				if(value instanceof Integer){
					preparedStatement.setInt(i+1, (Integer)value) ;
				}else if(value instanceof Long){
					preparedStatement.setLong(i+1, (Long)value) ;
				}else if(value instanceof Float){
					preparedStatement.setFloat(i+1, (Float)value) ;
				}else if(value instanceof Double){
					preparedStatement.setDouble(i+1, (Double)value) ;
				}else if(value instanceof Date){
					preparedStatement.setDate(i+1, (Date)value);
				}else if(value instanceof BigDecimal){
					preparedStatement.setBigDecimal(i+1, (BigDecimal)value);
				}else if(value instanceof String){
					preparedStatement.setString(i+1, value+"");
				}else{
					preparedStatement.setObject(i+1, value);
					
				}
				
			}
			long starTime = System.currentTimeMillis();
			ResultSet resultSetCount = preparedStatement.executeQuery();
			long endTime = System.currentTimeMillis();
			System.out.println("所需要花费的时间是："+(endTime-starTime));
			resultSetCount.last(); 
			int rowCount = resultSetCount.getRow(); //鑾峰緱ResultSet鐨勬�琛屾暟
			DbUtil.close(null, null, resultSetCount);
			
			sql = this.sqlStar + sql + this.sqlEnd ;
			System.out.println("--->>>sql:"+sql);
			System.out.println("--->>>valuesList:"+valuesList);
			preparedStatement = connection.prepareStatement(sql);
			for (int i = 0; i <valuesList.size(); i++) {
				Object value = valuesList.get(i);
				if(value instanceof Integer){
					preparedStatement.setInt(i+1, (Integer)value) ;
				}else if(value instanceof Long){
					preparedStatement.setLong(i+1, (Long)value) ;
				}else if(value instanceof Float){
					preparedStatement.setFloat(i+1, (Float)value) ;
				}else if(value instanceof Double){
					preparedStatement.setDouble(i+1, (Double)value) ;
				}else if(value instanceof Date){
					preparedStatement.setDate(i+1, (Date)value);
				}else if(value instanceof BigDecimal){
					preparedStatement.setBigDecimal(i+1, (BigDecimal)value);
				}else if(value instanceof String){
					preparedStatement.setString(i+1, value+"");
					
				}else{
					preparedStatement.setObject(i+1, value);
					
				}
				
			}
			ResultSet resultSet = preparedStatement.executeQuery();
			String executedQuery = resultSet.getStatement().toString();
			List<Map<String, Object>> datas = new ArrayList<Map<String,Object>>();
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			while(resultSet.next()){
				HashMap<String, Object> map = new HashMap<String, Object>();
				for (int i = 0; i < columnCount; i++) {
					String columnName = metaData.getColumnLabel(i + 1);
					if ("RN".equals(columnName.toUpperCase())) {
						continue;
					}
					if(resultSet.getObject(columnName) != null) {
						if (resultSet.getObject(columnName) instanceof Clob) {
							Clob clob = resultSet.getClob(columnName);
							String value = clob.getSubString((long) 1, (int) clob.length());
							map.put(columnName.toUpperCase(), value); 
						} else if (resultSet.getObject(columnName) instanceof Blob) {
							Blob blob = resultSet.getBlob(columnName);
							long len = blob.length();
							byte [] data = blob.getBytes(1,(int) len);
							map.put(columnName.toUpperCase(), data); 
						}  else {
							String value = resultSet.getObject(columnName).toString();
							map.put(columnName.toUpperCase(), value); 
						}
					} else {
						map.put(columnName.toUpperCase(), "");
					}
				}
				datas.add(map);
			}
			connection.commit();
			DbUtil.close(connection, preparedStatement, resultSet);
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("datas", datas);
			map.put("count", rowCount);
			String json = JSON.toJSON(map).toString();
	        System.out.println(json);
			return map ;
		}
}
