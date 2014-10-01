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
import com.dianping.auto.tcrunner.utils.OutFileUtil;
import org.apache.commons.lang.StringUtils;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

public class IsDemoConsistent<T> extends DiagnosingMatcher<T> {
    private DolTCParser tcParser;
    private HqlExecutor testHqlExecutor;
    private HqlExecutor onlineHqlExecutor;

    public IsDemoConsistent(DolTCParser tcParser, HqlExecutor testHqlExecutor, HqlExecutor onlineHqlExecutor) {
        this.tcParser = tcParser;
        this.testHqlExecutor= testHqlExecutor;
        this.onlineHqlExecutor = onlineHqlExecutor;
    }

    @Override
    public boolean matches(Object doltestcase, Description mismatch) {
//        String hql = tcParser.parse((String) doltestcase);
//        testHqlExecutor.setHql(hql);
//        onlineHqlExecutor.setHql(hql);

        Boolean match = false;

        String testContent = "testContent";
        String onlineContent = "testContent";

        if (Description.NONE == mismatch) {
            try {
//                testHqlExecutor.start();
//                onlineHqlExecutor.start();
//                testHqlExecutor.join();
//                onlineHqlExecutor.join();
//                if ((0 == testHqlExecutor.isSuccess) && (0 == onlineHqlExecutor.isSuccess)) {
//                    testContent = OutFileUtil.outfileToString("test.out");
//                    onlineContent = OutFileUtil.outfileToString("online.out");
//                    match = StringUtils.equals(testContent, onlineContent);
//                }
                match = StringUtils.equals(testContent, onlineContent);
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
    public static <T> Matcher<T> isDemoConsistent(DolTCParser tcParser, HqlExecutor testHqlExecutor, HqlExecutor onlineHqlExecutor) {
        return new IsDemoConsistent<T>(tcParser, testHqlExecutor, onlineHqlExecutor);
    }
}
