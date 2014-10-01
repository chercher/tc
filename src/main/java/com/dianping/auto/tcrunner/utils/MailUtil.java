package com.dianping.auto.tcrunner.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;
import java.util.Date;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-17
 * Time: 上午11:00
 * To change this template use File | Settings | File Templates.
 */
public class MailUtil {
    private static final Logger logger = LoggerFactory.getLogger(MailUtil.class.getName());

    private static final String MAIL_SUBJECT = "TC Report";
    private static final String MAIL_SMTP_HOST_LABEL = "mail.smtp.host";
    private static final String MAIL_SMTP_HOST = "10.100.100.101";
    private static final String MAIL_SMTP_AUTH_LABEL = "mail.smtp.auth";
    private static final String MAIL_SMTP_AUTH = "false";
    private static final String MAIL_51PING = "dpbi@51ping.com";

    public static void sendTCReportMail(String email, String message) {
        Properties props = new Properties();
        props.put(MAIL_SMTP_HOST_LABEL, MAIL_SMTP_HOST);
        props.put(MAIL_SMTP_AUTH_LABEL, MAIL_SMTP_AUTH);
        Session session = Session.getInstance(props);
        MimeMessage msg = new MimeMessage(session);

        try {
            msg.setFrom(new InternetAddress(MAIL_51PING));
            msg.addRecipients(Message.RecipientType.TO, email);
            msg.setSubject(MAIL_SUBJECT);
            msg.setSentDate(new Date());
            Multipart multipart = new MimeMultipart();
            BodyPart contentPart = new MimeBodyPart();
            contentPart.setContent(message, "text/html;charset=utf8");
            multipart.addBodyPart(contentPart);

            BodyPart attachBodyPart_1 = new MimeBodyPart();
            DataSource testout = new FileDataSource("test.out");
            attachBodyPart_1.setDataHandler(new DataHandler(testout));
            attachBodyPart_1.setFileName("test.out");
            multipart.addBodyPart(attachBodyPart_1);

            BodyPart attachBodyPart_2 = new MimeBodyPart();
            DataSource onlineout = new FileDataSource("online.out");
            attachBodyPart_2.setDataHandler(new DataHandler(onlineout));
            attachBodyPart_2.setFileName("online.out");
            multipart.addBodyPart(attachBodyPart_2);

            msg.setContent(multipart);
            Transport.send(msg);
        } catch (MessagingException e) {
            System.err.println(e.getMessage());
            logger.error(e.getMessage());
        }
    }

    public static void main(String[] args) {
        MailUtil.sendTCReportMail("pansy.wang@dianping.com", "test mail");
    }

}
