package com.sankuai.inf.leaf.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * 用于获取本地 ip（如果本机有多块网卡，这个方法可能会有缺陷）
 */
public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);
    public static String getIp() {
        String ip;
        try {   // 获取本机的 ip address
            InetAddress addr = InetAddress.getLocalHost();
            ip = addr.getHostAddress();
        } catch(Exception ex) {
            ip = "";
            logger.warn("Utils get IP warn", ex);
        }
        return ip;
    }
}
