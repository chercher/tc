package com.dianping.auto.tcrunner.core;

import com.dianping.auto.tcrunner.enums.ColumnTypeEnum;
import org.apache.hadoop.hive.conf.HiveConf;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shanshan.jin on 14-9-17.
 */
public class   {
    private ResultSet getResultSet(HiveConf conf,String tableName) throws ClassNotFoundException, SQLException {
        
        String mysqldb = conf.get("javax.jdo.option.ConnectionURL");
        String user = conf.get("javax.jdo.option.ConnectionUserName");
        String password = conf.get("javax.jdo.option.ConnectionPassword");
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection(mysqldb, user, password);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select a.COLUMN_NAME,a.TYPE_NAME from COLUMNS_V2 a, TBLS b, SDS c "
                    + " where a.CD_ID=c.CD_ID and b.SD_ID = c.SD_ID and b.TBL_NAME='" + tableName.toLowerCase() + "'"
                    + "and b.DB_ID = 22 order by INTEGER_IDX");
        
        return rs;
    }

    private List<String> getAllColumns(HiveConf conf,String tableName) throws ClassNotFoundException, SQLException {
        ResultSet rs = getResultSet(conf, tableName);
        List<String> allColumns = new ArrayList<String>();
        while (rs.next()) {
            String column = rs.getString(1).trim().toLowerCase;
            allColumns.add(column);
        }
        return allColumns;    
    }

    public String genSumCountHql(HiveConf conf,String tableName) throws SQLException, ClassNotFoundException {
        ResultSet rs = getResultSet(conf, tableName);
        List<String> sumColumns = new ArrayList<String>();
        boolean isSumType;
        while (rs.next()) {
            isSumType=false;
            String columnType = rs.getString(2).trim().toLowerCase();
            for(ColumnTypeEnum columnTypeEnum : ColumnTypeEnum.values()){
                if (columnType.equals(columnTypeEnum.toString().toLowerCase())){
                    isSumType=true;
                }
            }
            if(isSumType){
                sumColumns.add(rs.getString(1).trim().toLowerCase());
            }

        }
        StringBuffer bf = new StringBuffer();
        bf.append("select ");
        for(String columnName : sumColumns){
           bf.append("sum("+columnName+"),");
        }
        bf.append("count(*) from  " + tableName);
        return bf.toString();
    }

    public String genOnlineHashHql(HiveConf conf,String tableName) throws SQLException, ClassNotFoundException {
        List<String> allColumns = getAllColumns(conf, tableName);
        StringBuffer bf = new StringBuffer();
        bf.append("select hash64(concat_null(");
        for(String columnName : allColumns) {
            bf.append("to_json(" + columnName + "),");
        }
        int last = bf.length;
        bf.delete(last, last);
        bf.append(")) from " + tableName + " order by rand() limit 10;");  
    }

   public String genTestHashCountHql(HiveConf conf,String tableName, String concatHash) throws SQLException, ClassNotFoundException {
        List<String> allColumns = getAllColumns(conf, tableName);
        StringBuffer bf = new StringBuffer();
        bf.append("select count(*) from " + tableName + " where hash64(concat_null(");
        for (String columnName : allColumns) {
            bf.append("to_json(" + columnName + "), ");
        }
        int last = bf.length;
        bf.delete(last, last);
        bf.append(")) in " + concatHash);
   }
}
