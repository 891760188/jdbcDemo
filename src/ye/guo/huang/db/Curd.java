package ye.guo.huang.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import ye.guo.huang.tool.DbUtil;

public class Curd {
	
	private List<Map<String, Object>> query() throws SQLException{
		String sql = "select a.* from EMP a  where a.deptno = ?";
		List param = new ArrayList();
		param.add(10);
		CommonDBOperate  dbQuery = new CommonDBOperate();
		dbQuery.getResultListOfPage2(sql, param);

		return null;
	}
	
	public static void main(String[] args) {
		Curd curd = new Curd();
		try {
			curd.query();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
