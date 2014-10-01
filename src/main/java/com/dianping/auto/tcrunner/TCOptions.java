package com.dianping.auto.tcrunner;

import com.dianping.auto.tcrunner.enums.TCTypeEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang.StringUtils.*;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-10
 * Time: 下午7:01
 * To change this template use File | Settings | File Templates.
 */
public class TCOptions {
    public TCTypeEnum tcTypeEnum;
    public String e1;
    public String e3;
    public String tablename;
    public String email ;
    public Integer numkeyfields;
    public String partition;
    public Integer numrows;

    public TCOptions() {
        this.numkeyfields = 1;
        this.numrows = 10;
        this.email = System.getProperty("user.name") + "@dianping.com";
    }

    public TCTypeEnum getTcTypeEnum() {
        return tcTypeEnum;
    }

    public void setTcTypeEnum(TCTypeEnum tcTypeEnum) {
        this.tcTypeEnum = tcTypeEnum;
    }

    public String gete1() {
        return e1;
    }

    public void sete1(String e1) {
        this.e1 = e1;
    }

    public String gete3() {
        return e3;
    }

    public void sete3(String e3) {
        this.e3 = e3;
    }

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Integer getNumkeyfields() {
        return numkeyfields;
    }

    public void setNumkeyfields(Integer numkeyfields) {
        this.numkeyfields = numkeyfields;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public Integer getNumrows() {
        return numrows;
    }

    public void setNumrows(Integer numrows) {
        this.numrows = numrows;
    }

    @Override
    public String toString() {
        StringBuilder tc = new StringBuilder("TC params:");
        if (null != gete1()) {
            tc.append(" -e1 ");
            tc.append(gete1());
        }
        if (null != gete3()) {
            tc.append(" -e3 ");
            tc.append(gete3());
        }
        if (null != getTablename()) {
            tc.append(" -t ");
            tc.append(getTablename());
        }
        if (1 != getNumkeyfields()) {
            tc.append(" -k ");
            tc.append(getNumkeyfields());
        }
        if (null != getPartition()) {
            tc.append(" -p ");
            tc.append(getPartition());
        }
        if (10 != getNumrows()) {
            tc.append(" -n ");
            tc.append(getNumrows());
        }

        return tc.toString();
    }
}
