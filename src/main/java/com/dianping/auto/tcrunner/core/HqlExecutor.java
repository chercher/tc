package com.dianping.auto.tcrunner.core;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-9
 * Time: 上午11:06
 * To change this template use File | Settings | File Templates.
 */
import com.dianping.auto.tcrunner.enums.HiveEnvEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.*;

public class HqlExecutor implements Callable<HashMap<Integer, String>> {
    private HiveConf conf;
    private CliSessionState ss;
    private CliDriver cliDriver;
//    public String result;
    public String isSuccess;
    private String hql;
    private HiveEnvEnum env;
    private CountDownLatch countDownLatch;
    private String postfix;

    public HqlExecutor(HiveEnvEnum env, CountDownLatch countDownLatch) {
        this(env, "", countDownLatch);
    }

    public HqlExecutor(HiveEnvEnum env, String hql, CountDownLatch countDownLatch) {
        this.env = env;
        this.hql = hql;
        this.countDownLatch = countDownLatch;
    }

    public void setHql(String hql) {
        this.hql = hql;
    }

    @Override
    public HashMap<Integer, String> call() {
        Thread.currentThread().setContextClassLoader(new URLClassLoader(new URL[] {}) {
            @Override
            public URL getResource(String name) {
                if (StringUtils.equals("hive-site.xml", name)) {
                    return super.getResource(env + "/hive-site.xml");
                }
                return super.getResource(name);
            }
        });

        conf = new HiveConf(Driver.class);
        conf.addResource(env + "/core-site.xml");
        conf.addResource(env + "/hdfs-site.xml");
        conf.addResource(env + "/mapred-site.xml");
        conf.addResource(env + "/hive-site.xml");
        UserGroupInformation.setConfiguration(conf);

        ss = new CliSessionState(conf);
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            postfix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis()));
            FileOutputStream fos = new FileOutputStream(env + ".out." + postfix);
//            FileOutputStream fes = new FileOutputStream(env + ".err." + postfix);
            ss.out = new PrintStream(fos, true, "UTF-8");
//            ss.err = new PrintStream(fes, true ,"UTF-8");
            ss.err = System.out;

            ss.initFiles.add(System.getenv("HIVE_CONF_DIR") + File.separator + ".hiverc");

            CliSessionState.start(ss);

            this.cliDriver = new CliDriver();
            synchronized (HqlExecutor.class) {
                Thread.sleep(10000);
                this.cliDriver.processInitFiles(ss);
            }

            this.isSuccess = String.valueOf(this.cliDriver.processLine(hql,true));

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            countDownLatch.countDown();
        }

        ss.close();

//        result = baos.toString();
//        System.out.println(env + ": " + result);
//        HashMap<Integer, String> result = new HashMap<Integer, String>(){{put(isSuccess, postfix);}};
        HashMap<Integer, String> result = new HashMap<Integer, String>(){};
        result.put(0, isSuccess);
        result.put(1, postfix);

        return result;
    }
//
//    public void close() {
//        ss.close();
//    }

    public static void main(String[] args) throws Exception {
        String hql1 = "use bi; select date_add('2014-03-28', 1) from dual";
        String hql2 = "use bi;select dp_date_add('2014-03-29', 1) from dual";

//        HqlExecutor testHqlExecutor2 = new HqlExecutor("test", HiveEnvEnum.TEST, hql1);
//        testHqlExecutor2.start();
//
//        HqlExecutor onlineHqlExecutor2 = new HqlExecutor("online", HiveEnvEnum.ONLINE, hql2);
//        onlineHqlExecutor2.start();
//
//        try {
//            testHqlExecutor2.join();
//            testHqlExecutor2.close();
//            onlineHqlExecutor2.join();
//            onlineHqlExecutor2.close();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        CountDownLatch countDownLatch1 = new CountDownLatch(2);
        CountDownLatch countDownLatch2 = new CountDownLatch(2);



        ExecutorService threadPool = Executors.newFixedThreadPool(2);
        threadPool.execute(new FutureTask<HashMap<Integer, String>>(new HqlExecutor(HiveEnvEnum.TEST, hql1, countDownLatch1)));
        threadPool.execute(new FutureTask<HashMap<Integer, String>>(new HqlExecutor(HiveEnvEnum.ONLINE, hql1, countDownLatch1)));

        countDownLatch1.await();

        threadPool.execute(new FutureTask<HashMap<Integer, String>>(new HqlExecutor(HiveEnvEnum.TEST, hql2, countDownLatch2)));
        threadPool.execute(new FutureTask<HashMap<Integer, String>>(new HqlExecutor(HiveEnvEnum.ONLINE, hql2, countDownLatch2)));

        countDownLatch2.await();

        threadPool.shutdown();

    }
}
