package com.dianping.auto.tcrunner.core;

import com.dianping.auto.tcrunner.utils.MailUtil;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-17
 * Time: 下午3:00
 * To change this template use File | Settings | File Templates.
 */
public class TCMatcherAssert {

    public static <T> void assertThat(T actual, Matcher<? super T> matcher, String email, String msg) {
        assertThat("", actual, matcher, email, msg);
    }

    public static <T> void assertThat(String reason, T actual, Matcher<? super T> matcher, String email, String msg) {
        String tcmsg = "";
        if (!matcher.matches(actual)) {
            Description description = new StringDescription();
            description.appendText(reason)
                    .appendText("<br>Expected: ")
                    .appendDescriptionOf(matcher)
                    .appendText("<br>&nbsp&nbsp&nbsp&nbspbut: ");
            matcher.describeMismatch(actual, description);

            tcmsg = msg + "<br>Test Result: FAIL" + description.toString();
//            throw new AssertionError(description.toString());
        } else {
            tcmsg = msg + "<br>Test Result: PASS";
        }
        HashMap<Integer, String> outfilePostfix = matcher.getPostfix();
        tcmsg = tcmsg + "<br>See attachments for more detailed execution result in separative environments.";
        MailUtil.sendTCReportMail(email, tcmsg);
    }

    public static void assertThat(String reason, boolean assertion) {
        if (!assertion) {
            throw new AssertionError(reason);
        }
    }
}
