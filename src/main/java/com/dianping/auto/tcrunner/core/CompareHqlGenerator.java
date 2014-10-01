package com.dianping.auto.tcrunner.core;

import com.dianping.auto.tcrunner.enums.ColumnTypeEnum;
import org.apache.hadoop.hive.conf.HiveConf;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shanshan.jin on 14-9-17.
 */
public class CompareHqlGenerator {
    private List<String> getSumColumns(HiveConf conf,String tableName) throws ClassNotFoundException, SQLException {
        List<String> sumColumns = new ArrayList<String>();
        String mysqldb = conf.get("javax.jdo.option.ConnectionURL");
        String user = conf.get("javax.jdo.option.ConnectionUserName");
        String password = conf.get("javax.jdo.option.ConnectionPassword");
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection(mysqldb, user, password);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select a.COLUMN_NAME,a.TYPE_NAME from COLUMNS_V2 a, TBLS b, SDS c "
                    + " where a.CD_ID=c.CD_ID and b.SD_ID = c.SD_ID and b.TBL_NAME='" + tableName.toLowerCase() + "'"
                    + "and b.DB_ID = 22 order by INTEGER_IDX");
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
        return sumColumns;
    }

    public String generate(HiveConf conf,String tableName) throws SQLException, ClassNotFoundException {
        StringBuffer bf = new StringBuffer();
        bf.append("select ");
        List<String> sumColumns = getSumColumns(conf,tableName);
        for(String columnName : sumColumns){
           bf.append("sum("+columnName+"),");
        }
        bf.append("count(*) from  " + tableName);
        return bf.toString();
    }
}
