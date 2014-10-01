package com.dianping.auto.tcrunner.enums;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-10
 * Time: 下午7:05
 * To change this template use File | Settings | File Templates.
 */
public enum TCTypeEnum {
    TC1("TC1"), TC3("TC3"), TC5("TC5");
    private String str;
    private TCTypeEnum(String str) {
        this.str = str;
    }
    public String toString() {
        return str;
    }
}
