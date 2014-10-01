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
import org.apache.commons.lang.StringUtils;
import org.hamcrest.*;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public class IsOneLineConsistent<T> extends DiagnosingMatcher<T> {
    private DolTCParser tcParser;
    private CountDownLatch countDownLatch;
    private HqlExecutor testHqlExecutor;
    private HqlExecutor onlineHqlExecutor;
    private FutureTask<HashMap<Integer, String>> testFutureTask;
    private FutureTask<HashMap<Integer, String>> onlineFutureTask;
    private ExecutorService threadPool;
    private HashMap<Integer, String> outfilePostfix;

    public IsOneLineConsistent() {
        tcParser = new DolTCParser();
        countDownLatch = new CountDownLatch(2);
        testHqlExecutor = new HqlExecutor(HiveEnvEnum.TEST, countDownLatch);
        testFutureTask = new FutureTask<HashMap<Integer, String>>(testHqlExecutor);
        onlineHqlExecutor = new HqlExecutor(HiveEnvEnum.ONLINE, countDownLatch);
        onlineFutureTask = new FutureTask<HashMap<Integer, String>>(onlineHqlExecutor);
        threadPool = Executors.newFixedThreadPool(2);
    }

    public HashMap<Integer, String> getPostfix() {
        return outfilePostfix;
    }

    @Override
    public boolean matches(Object e1, Description mismatch) {
        String hql = tcParser.parse((String) e1);
        testHqlExecutor.setHql(hql);
        onlineHqlExecutor.setHql(hql);

        Boolean match = false;

        String testContent = "";
        String onlineContent = "";

        if (Description.NONE == mismatch) {
            try {
                threadPool.execute(testFutureTask);
                threadPool.execute(onlineFutureTask);
                countDownLatch.await();
                outfilePostfix.put(0, testFutureTask.get().get(0));
                outfilePostfix.put(1, onlineFutureTask.get().get(1));
                if (("0" == testFutureTask.get().get(0)) && ("0" == onlineFutureTask.get().get(0))) {
                    testContent = OutFileUtil.outfileToString("test.out" + testFutureTask.get().get(1));
                    onlineContent = OutFileUtil.outfileToString("online.out" + onlineFutureTask.get().get(1));
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
        description.appendText("is consistent across test and online.");
    }

    @Factory
    public static <T> Matcher<T> isOneLineConsistent() {
        return new IsOneLineConsistent<T>();
    }
}
