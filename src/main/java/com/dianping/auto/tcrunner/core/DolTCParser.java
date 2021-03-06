package com.dianping.auto.tcrunner.core;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-9
 * Time: 上午11:05
 * To change this template use File | Settings | File Templates.
 */
import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.conf.CanaanConf;
import com.dianping.data.warehouse.canaan.dolite.DateTimeContext;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.context.Context;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DolTCParser {

    public String parse(String doltc) {
        Context context = new VelocityContext();
        StringWriter writer = new StringWriter();
        Properties calProp = new Properties();
        CanaanConf conf = new CanaanConf();

        for (Constants.BATCH_CAL_VARS var: Constants.BATCH_CAL_VARS.values()) {
            String key = var.toString();
            String value = conf.getCalVariables(key);
            calProp.setProperty(key, value);
        }
        Map<String, String> env =  new HashMap<String, String>();
        for (Object key : calProp.keySet()) {
            env.put(key.toString(), calProp.get(key).toString());
        }
        context.put("env", env);
        DateTimeContext dtCtx = new DateTimeContext();
        context.put("dt", dtCtx);
        Velocity.evaluate(context, writer, getClass().getSimpleName(), doltc);
        return writer.toString();
    }

    public static void main(String[] args) {
        String doltc = new String("select *; set mapred.job.name = drpt_tg_mobile_sm_${env.YYYY}${env.MM};DROP TABLE IF EXISTS dpstg_tg_mobile_sales_amt_${env.YYYY}${env.MM};");
        DolTCParser tcParser = new DolTCParser();
        String result = tcParser.parse(doltc);

        System.out.println(result);
    }
}

