package com.sankuai.inf.leaf.server;

import com.alibaba.druid.pool.DruidDataSource;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.PropertyFactory;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.ZeroIDGen;
import com.sankuai.inf.leaf.segment.SegmentIDGenImpl;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.dao.impl.IDAllocDaoImpl;
import com.sankuai.inf.leaf.server.exception.InitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.Properties;

@Service("SegmentService")
public class SegmentService {
    private Logger logger = LoggerFactory.getLogger(SegmentService.class);
    IDGen idGen;
    DruidDataSource dataSource;
    public SegmentService() throws SQLException, InitException {    // 根据配置文件来决定是初始化 ZeroIDGen，还是 SegmentIDGenImpl，如果是后者，就
        Properties properties = PropertyFactory.getProperties();    // 初始化 dataSource，构建 SegmentIDGenImpl 并初始化（主要是完成了从数据库中获取全部的 biz-tag 信息再更新缓存的过程，另外启动了一个每分钟从数据库更新缓存的任务）
        boolean flag = Boolean.parseBoolean(properties.getProperty(Constants.LEAF_SEGMENT_ENABLE, "true"));
        if (flag) {


            // Config dataSource
            dataSource = new DruidDataSource();
            dataSource.setUrl(properties.getProperty(Constants.LEAF_JDBC_URL));
            dataSource.setUsername(properties.getProperty(Constants.LEAF_JDBC_USERNAME));
            dataSource.setPassword(properties.getProperty(Constants.LEAF_JDBC_PASSWORD));
            dataSource.init();

            // Config Dao
            IDAllocDao dao = new IDAllocDaoImpl(dataSource);

            // Config ID Gen
            idGen = new SegmentIDGenImpl();
            ((SegmentIDGenImpl) idGen).setDao(dao);
            if (idGen.init()) { // 初始化，主要是完成了从数据库中获取全部的 biz-tag 信息再更新缓存的过程，另外启动了一个每分钟从数据库更新缓存的任务
                logger.info("Segment Service Init Successfully");
            } else {
                throw new InitException("Segment Service Init Fail");
            }
        } else {
            idGen = new ZeroIDGen();
            logger.info("Zero ID Gen Service Init Successfully");
        }
    }   // 通过 idGen 获取 id
    public Result getId(String key) {
        return idGen.get(key);  // 通过 idGen 获取 id
    }
    public SegmentIDGenImpl getIdGen() {
        if (idGen instanceof SegmentIDGenImpl) {
            return (SegmentIDGenImpl) idGen;
        }
        return null;
    }
}
