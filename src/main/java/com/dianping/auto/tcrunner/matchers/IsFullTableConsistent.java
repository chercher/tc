package com.dianping.auto.tcrunner.matchers;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-9
 * Time: 上午11:03
 * To change this template use File | Settings | File Templates.
 */


import com.dianping.auto.tcrunner.core.CompareHqlGenerator;
import com.dianping.auto.tcrunner.core.HqlExecutor;
import com.dianping.auto.tcrunner.enums.HiveEnvEnum;
import com.dianping.auto.tcrunner.utils.OutFileUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public class IsFullTableConsistent<T> extends DiagnosingMatcher<T> {
    private CountDownLatch countDownLatch1;
    private CountDownLatch countDownLatch2;
    private CountDownLatch countDownLatch3;
    private HqlExecutor testHqlExecutor1;
    private HqlExecutor testHqlExecutor2;
    private HqlExecutor onlineHqlExecutor1;
    private HqlExecutor onlineHqlExecutor2;
    private FutureTask<HashMap<Integer, String>> testFutureTask1;
    private FutureTask<HashMap<Integer, String>> testFutureTask2;
    private FutureTask<HashMap<Integer, String>> onlineFutureTask1;
    private FutureTask<HashMap<Integer, String>> onlineFutureTask2;
    private ExecutorService threadPool;
    private HiveConf conf =  new HiveConf(Hive.class);

    public IsFullTableConsistent() {
        countDownLatch1 = new CountDownLatch(2);
        countDownLatch2 = new CountDownLatch(1);
        countDownLatch3 = new CountDownLatch(1);
        testHqlExecutor1 = new HqlExecutor(HiveEnvEnum.TEST, countDownLatch1);
        testFutureTask1 = new FutureTask<HashMap<Integer, String>>(testHqlExecutor1);
        testHqlExecutor2 = new HqlExecutor(HiveEnvEnum.TEST, countDownLatch2);
        testFutureTask2 = new FutureTask<HashMap<Integer, String>>(testHqlExecutor2);
        onlineHqlExecutor1 = new HqlExecutor(HiveEnvEnum.ONLINE, countDownLatch1);
        onlineFutureTask1 = new FutureTask<HashMap<Integer, String>>(onlineHqlExecutor1);
        onlineHqlExecutor2 = new HqlExecutor(HiveEnvEnum.ONLINE, countDownLatch2);
        onlineFutureTask2 = new FutureTask<HashMap<Integer, String>>(onlineHqlExecutor2);
        threadPool = Executors.newFixedThreadPool(2);
    }

    @Override
    public boolean matches(Object tableName, Description mismatch) {
        String hql = null;
        try {
            hql = FulltableHqlGenerator.genSumCountHql(conf,(String) tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        testHqlExecutor1.setHql(hql);
        onlineHqlExecutor1.setHql(hql);

        Boolean match = false;
        String testContent = "";
        String onlineContent = "";

        if (Description.NONE == mismatch) {
            try {
                threadPool.execute(testFutureTask1);
                threadPool.execute(onlineFutureTask1);
                countDownLatch1.await();
                if (("0" == testFutureTask1.get().get(0)) && ("0" == onlineFutureTask1.get().get(0))) {
                    testContent = OutFileUtil.outfileToString("test.out" + testFutureTask1.get().get(1));
                    onlineContent = OutFileUtil.outfileToString("online.out" + onlineFutureTask1.get().get(1));
                    match = StringUtils.equals(testContent, onlineContent);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        

        if (!match) {
            mismatch.appendText("test is ").appendValue(testContent).appendText(" while online is ").appendValue(onlineContent);
        }

        return match;

    }

    @Override
    public void describeTo(Description description) {
        description.appendText("is consistent across 7.32 and online.");
    }

    @Factory
    public static <T> Matcher<T> isFullTableConsistent() {
        return new IsFullTableConsistent<T>();
    }
}
