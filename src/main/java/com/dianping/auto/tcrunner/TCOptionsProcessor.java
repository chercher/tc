package com.dianping.auto.tcrunner;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-9
 * Time: 上午11:21
 * To change this template use File | Settings | File Templates.
 */

import com.dianping.auto.tcrunner.enums.TCTypeEnum;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class TCOptionsProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TCOptionsProcessor.class.getName());
    private final Options options = new Options();
    private CommandLine cmdline;

    public TCOptionsProcessor() {
        Option e1 = Option.builder("e1").hasArg().argName("tc1dolhql").desc("dol hql of TC1 type").required(false).build();
        Option e3 = Option.builder("e3").hasArg().argName("tc3dolhql").desc("dol hql of TC3 type").required(false).build();
        Option t = Option.builder("t").hasArg().argName("tablename").desc("hive table name to be compared").required(false).build();

        OptionGroup tctype = new OptionGroup();
        tctype.addOption(e1);
        tctype.addOption(e3);
        tctype.addOption(t);
        options.addOptionGroup(tctype);

        Option m = Option.builder("m").hasArg().argName("email").required(false).desc("email address, default to userid@dianping.com").build();
        options.addOption(m);

        Option p = Option.builder("p").hasArg().argName("partition").required(false).desc("partition key value, e.g. hp_cal_dt='2014-09-11'").build();
        options.addOption(p);

        Option n = Option.builder("n").hasArg().argName("numrows").required(false).desc("num of selected data rows to be compared, defalut to ten").build();
        options.addOption(n);

        Option h = Option.builder("h").hasArg(false).required(false).desc("print help information").build();
        options.addOption(h);
    }

    private void printUsage() {
        new HelpFormatter().printHelp("tc", options);
    }

    public boolean process(String[] argv, TCOptions tcoptions) {
        try {
            cmdline = new DefaultParser().parse(options, argv);
            if (cmdline.hasOption("h")) {
                printUsage();
                return false;
            }

            if (cmdline.hasOption("e1")) {
                tcoptions.setTcTypeEnum(TCTypeEnum.TC1);
            } else if (cmdline.hasOption("e3")) {
                tcoptions.setTcTypeEnum(TCTypeEnum.TC3);
            } else if (cmdline.hasOption("t")) {
                tcoptions.setTcTypeEnum(TCTypeEnum.TC5);
            }

            tcoptions.sete1(cmdline.getOptionValue("e1"));
            tcoptions.sete3(cmdline.getOptionValue("e3"));
            tcoptions.setTablename(cmdline.getOptionValue("t"));
            tcoptions.setEmail(cmdline.getOptionValue("m"));
            tcoptions.setPartition(cmdline.getOptionValue("p"));
            String numrows = cmdline.getOptionValue("n");
            if (null != numrows) {
                tcoptions.setNumrows(Integer.parseInt(numrows));
            }

        } catch (ParseException e) {
            System.err.println(e.getMessage());
            logger.error(e.getMessage());
            printUsage();
            return false;
        }
        return true;
    }


}