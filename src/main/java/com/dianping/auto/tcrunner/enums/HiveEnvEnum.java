package com.dianping.auto.tcrunner.enums;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-9
 * Time: 上午11:06
 * To change this template use File | Settings | File Templates.
 */
public enum HiveEnvEnum {
    TEST("test"), ONLINE("online");
    private String str;
    private HiveEnvEnum(String str) {
        this.str = str;
    }
    public String toString() {
        return str;
    }
}
