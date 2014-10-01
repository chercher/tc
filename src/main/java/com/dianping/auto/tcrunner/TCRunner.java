package com.dianping.auto.tcrunner;

import com.dianping.auto.tcrunner.core.CompareHqlGenerator;
import com.dianping.auto.tcrunner.core.DolTCParser;
import com.dianping.auto.tcrunner.core.HqlExecutor;
import com.dianping.auto.tcrunner.enums.HiveEnvEnum;
import com.dianping.auto.tcrunner.enums.TCTypeEnum;

import static com.dianping.auto.tcrunner.core.TCMatcherAssert.assertThat;
import static com.dianping.auto.tcrunner.matchers.IsOneLineConsistent.isOneLineConsistent;
import static com.dianping.auto.tcrunner.matchers.IsMultiLinesConsistent.isMultiLinesConsistent;
import static com.dianping.auto.tcrunner.matchers.IsFullTableConsistent.isFullTableConsistent;
import static com.dianping.auto.tcrunner.matchers.IsDemoConsistent.isDemoConsistent;


/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-11
 * Time: 下午12:02
 * To change this template use File | Settings | File Templates.
 */
public class TCRunner {
//    protected DolTCParser tcParser;
//    protected HqlExecutor testHqlExecutor;
//    protected HqlExecutor onlineHqlExecutor;
//    protected CompareHqlGenerator generator;
//
//    public TCRunner() {
//        setup();
//    }
//
//    private void setup() {
//        tcParser = new DolTCParser();
//        testHqlExecutor = new HqlExecutor("test", HiveEnvEnum.TEST);
//        onlineHqlExecutor = new HqlExecutor("online", HiveEnvEnum.ONLINE);
//        generator = new CompareHqlGenerator();
//    }

//    private void AssertDemoConsistent(String doltestcase, String email, String msg) {
//        assertThat(doltestcase, isDemoConsistent(tcParser, testHqlExecutor, onlineHqlExecutor), email, msg);
//    }

    private void AssertOneLineConsistent(String e1, String email, String msg) {
        assertThat(e1, isOneLineConsistent(), email, msg);
    }


    private void AssertMultiLinesConsistent(String e3, Integer k, String email, String msg) {
        assertThat(e3, isMultiLinesConsistent(k), email, msg);
    }

    private void AssertFullTableConsistent(String t, String email, String msg) {
        assertThat(t, isFullTableConsistent(), email, msg);
    }


//    private void teardown() {
//        if (null != testHqlExecutor) {
//            if (Integer.MAX_VALUE != testHqlExecutor.isSuccess) {
//                testHqlExecutor.close();
//            }
//        }
//        if (null != onlineHqlExecutor) {
//            if (Integer.MAX_VALUE != onlineHqlExecutor.isSuccess)
//                onlineHqlExecutor.close();
//        }
//    }

    public int run(String[] args) {
        int ret = 0;

        TCOptionsProcessor tcoproc = new TCOptionsProcessor();
        TCOptions tco = new TCOptions();
        if (!tcoproc.process(args, tco)) {
            ret = 1;
        }

        TCTypeEnum tctype = tco.getTcTypeEnum();
        String email = tco.getEmail();
        String msg = tco.toString();

        switch (tctype) {
            case TC1:
                String e1 = tco.gete1();
                AssertOneLineConsistent(e1, email, msg);
                break;
            case TC3:
                String e3 = tco.gete3();
                Integer k = tco.getNumkeyfields();
                AssertMultiLinesConsistent(e3, k, email, msg);
                break;
            case TC5:
                String t = tco.getTablename();
                AssertFullTableConsistent(t, email, msg);
                break;
        }

//        teardown();
        return ret;
    }

    public static void main(String args[]) {

//        String[] arge1 = new String[] {"-e1", "xx", "-t", "yy", "-m", "pansy.wang@dianping.com"};
        String[] args1 = new String[] {"-e1", "use bi; select date_add('2014-03-28', 1) from dual", "-m", "pansy.wang@dianping.com"};
//        String[] args1 = new String[] {"-e3", "yy", "-m", "pansy.wang@dianping.com", "-h"};
//        String[] args1 = new String[] {"-t", "yy", "-p", "hp_cal_dt='2014-09-11'", "-m", "wang@dianping.com"};
//        String[] args1 = new String[] {"-t", "yy", "-p", "hp_cal_dt='2014-09-11'", "-k", "id,name", "-m", "wang@dianping.com"};
        TCRunner tcRunner = new TCRunner();
        int ret = tcRunner.run(args1);
        System.exit(ret);
    }
}
