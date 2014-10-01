package com.dianping.auto.tcrunner.matchers;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-9
 * Time: 上午11:03
 * To change this template use File | Settings | File Templates.
 */

import com.dianping.auto.tcrunner.core.DolTCParser;
import com.dianping.auto.tcrunner.core.HqlExecutor;
import com.dianping.auto.tcrunner.enums.HiveEnvEnum;
import com.dianping.auto.tcrunner.utils.OutFileUtil;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public class IsMultiLinesConsistent<T> extends DiagnosingMatcher<T> {
    private DolTCParser tcParser;
    private CountDownLatch countDownLatch;
    private HqlExecutor testHqlExecutor;
    private HqlExecutor onlineHqlExecutor;
    private FutureTask<HashMap<Integer, String>> testFutureTask;
    private FutureTask<HashMap<Integer, String>> onlineFutureTask;
    private ExecutorService threadPool;
    private Integer k;

    public IsMultiLinesConsistent(Integer k) {
        tcParser = new DolTCParser();
        countDownLatch = new CountDownLatch(2);
        testHqlExecutor = new HqlExecutor(HiveEnvEnum.TEST, countDownLatch);
        testFutureTask = new FutureTask<HashMap<Integer, String>>(testHqlExecutor);
        onlineHqlExecutor = new HqlExecutor(HiveEnvEnum.ONLINE, countDownLatch);
        onlineFutureTask = new FutureTask<HashMap<Integer, String>>(onlineHqlExecutor);
        threadPool = Executors.newFixedThreadPool(2);
        this.k = k;
    }

    @Override
    public boolean matches(Object e3, Description mismatch) {
        String hql = tcParser.parse((String) e3);
        testHqlExecutor.setHql(hql);
        onlineHqlExecutor.setHql(hql);

        Boolean match = false;

        if (Description.NONE == mismatch) {
            try {
                threadPool.execute(testFutureTask);
                threadPool.execute(onlineFutureTask);
                countDownLatch.await();
                if (("0" == testFutureTask.get().get(0)) && ("0" == onlineFutureTask.get().get(0))) {
                    Map<ArrayList<String>, ArrayList<Float>> testoutmap = OutFileUtil.outfileToMap("test.out" + testFutureTask.get().get(1), k);
                    Map<ArrayList<String>, ArrayList<Float>> onlineoutmap = OutFileUtil.outfileToMap("online.out" + onlineFutureTask.get().get(1), k);
                    match = testoutmap.equals(onlineoutmap);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (!match) {
            mismatch.appendText("test is ").appendValue("test.out").appendText(" while online is ").appendValue("online.out");
        }

        return match;

    }

    @Override
    public void describeTo(Description description) {
        description.appendText("is consistent across test and online.");
    }

    @Factory
    public static <T> Matcher<T> isMultiLinesConsistent(Integer k) {
        return new IsMultiLinesConsistent<T>(k);
    }
}
