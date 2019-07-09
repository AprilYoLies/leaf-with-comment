package com.sankuai.inf.leaf.server;

import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.PropertyFactory;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.ZeroIDGen;
import com.sankuai.inf.leaf.server.exception.InitException;
import com.sankuai.inf.leaf.snowflake.SnowflakeIDGenImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service("SnowflakeService")
public class SnowflakeService {
    private Logger logger = LoggerFactory.getLogger(SnowflakeService.class);
    IDGen idGen;
    public SnowflakeService() throws InitException {
        Properties properties = PropertyFactory.getProperties();    // 根据配置文件构建 Properties
        boolean flag = Boolean.parseBoolean(properties.getProperty(Constants.LEAF_SNOWFLAKE_ENABLE, "true"));
        if (flag) { // 如果开启了 snowflake 模式
            String zkAddress = properties.getProperty(Constants.LEAF_SNOWFLAKE_ZK_ADDRESS); // 获取注册中心地址
            int port = Integer.parseInt(properties.getProperty(Constants.LEAF_SNOWFLAKE_PORT)); // 获取注册中心的端口号
            idGen = new SnowflakeIDGenImpl(zkAddress, port);    // 主要是构建了 SnowflakeZookeeperHolder，负责和 zookeeper 的交互，然后尝试从 zookeeper 获取 worker id，失败的情况下从本地获取 worker id
            if(idGen.init()) {  // 如果服务启动完成，日志通知
                logger.info("Snowflake Service Init Successfully");
            } else {
                throw new InitException("Snowflake Service Init Fail");
            }
        } else {    // 如果没有开启 snowflake 模式，则默认使用 ZeroIDGen，它无限生成 id 0
            idGen = new ZeroIDGen();
            logger.info("Zero ID Gen Service Init Successfully");
        }
    }   // 获取时间戳，和上一个 id 的时间戳比对，如果不一致，跟 worker id 和序列号拼凑成 id 返回，否则变更序列号，再拼凑出 id 返回，如果变更序列号可能导致 id 重复，那么人为将时间戳等待到之后的时刻，再拼凑 id 返回
    public Result getId(String key) {
        return idGen.get(key);
    }
}
