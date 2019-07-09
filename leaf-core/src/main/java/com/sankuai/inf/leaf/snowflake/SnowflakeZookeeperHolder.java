package com.sankuai.inf.leaf.snowflake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sankuai.inf.leaf.snowflake.exception.CheckLastTimeException;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.sankuai.inf.leaf.common.*;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.zookeeper.CreateMode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class SnowflakeZookeeperHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeZookeeperHolder.class);
    private String zk_AddressNode = null;//保存自身的key  ip:port-000000001
    private String listenAddress = null;//保存自身的key ip:port
    private int workerID;
    private static final String PREFIX_ZK_PATH = "/snowflake/" + PropertyFactory.getProperties().getProperty("leaf.name");  // zookeeper 前缀 /snowflake/com.sankuai.leaf.opensource.test
    private static final String PROP_PATH = System.getProperty("java.io.tmpdir") + File.separator + PropertyFactory.getProperties().getProperty("leaf.name") + "/leafconf/{port}/workerID.properties";  // /var/folders/nf/g5tl2vq5607489_qhtx5_4cr0000gn/T//com.sankuai.leaf.opensource.test/leafconf/{port}/workerID.properties
    private static final String PATH_FOREVER = PREFIX_ZK_PATH + "/forever";//保存所有数据持久的节点    /snowflake/com.sankuai.leaf.opensource.test/forever
    private String ip;
    private String port;
    private String connectionString;
    private long lastUpdateTime;

    public SnowflakeZookeeperHolder(String ip, String port, String connectionString) {
        this.ip = ip;   // ip 缓存的是本机 ip
        this.port = port;   // zookeeper 的端口
        this.listenAddress = ip + ":" + port;
        this.connectionString = connectionString;   // zookeeper 的地址
    }
    // 这里就是一个尝试从 zookeeper 中获取 worker id 的过程，如果在获取 worker id 的过程中出现错误，那么就使用本地缓存的 worker id
    public boolean init() {
        try {   // 创建 curator 客户端，连接超时时间 10000ms，会话超时时间 6000ms
            CuratorFramework curator = createWithOptions(connectionString, new RetryUntilElapsed(1000, 4), 10000, 6000);
            curator.start();
            Stat stat = curator.checkExists().forPath(PATH_FOREVER);    // 查看持有路径是否存在
            if (stat == null) { // 如果持久路径不存在，则进行创建，更新本机 worker id，设置一个定时任务，每隔 3s 根据当前时间戳构建 data 写入 zookeeper，更新 lastUpdateTime
                //不存在根节点,机器第一次启动,创建/snowflake/ip:port-000000000,并上传数据
                zk_AddressNode = createNode(curator);   // 创建持久顺序节点 ,并把节点数据放入 value，这里的数据是 buildData 方法构建 EndPoint，持有了本机 ip，zookeeper 端口，时间戳，然后将其转换为 json 串
                //worker id 默认是0
                updateLocalWorkerID(workerID);  // 尝试更新本地的 worker id 文件，存在的情况下直接追加，否则创建后在写入内容
                //定时上报本机时间给forever节点
                ScheduledUploadData(curator, zk_AddressNode);   // 构建 SingleThreadScheduledExecutor，每隔 3s 检查是否发生时间回调，如果是，直接返回，否则根据当前时间戳构建 data 写入 zookeeper，更新 lastUpdateTime
                return true;
            } else {    // 执行到这里，说明持久路径是存在的
                Map<String, Integer> nodeMap = Maps.newHashMap();//ip:port->00001
                Map<String, String> realNode = Maps.newHashMap();//ip:port->(ipport-000001)
                //存在根节点,先检查是否有属于自己的根节点
                List<String> keys = curator.getChildren().forPath(PATH_FOREVER);    // 获取永久节点下的内容
                for (String key : keys) {
                    String[] nodeKey = key.split("-");
                    realNode.put(nodeKey[0], key);  // 存放的是 192.168.1.111:2181 -> 192.168.1.111:2181-0000000000
                    nodeMap.put(nodeKey[0], Integer.parseInt(nodeKey[1]));  // 存放的是 192.168.1.111:2181 -> 0000000000
                }
                Integer workerid = nodeMap.get(listenAddress);  // 得到自己的 worker id
                if (workerid != null) {
                    //有自己的节点,zk_AddressNode=ip:port
                    zk_AddressNode = PATH_FOREVER + "/" + realNode.get(listenAddress);  // 带 worker id 的全限定路径
                    workerID = workerid;//启动worder时使用会使用    记录自己的 worker id
                    if (!checkInitTimeStamp(curator, zk_AddressNode))   // 检查是否发生过时间回拨
                        throw new CheckLastTimeException("init timestamp check error,forever node timestamp gt this node time");
                    //准备创建临时节点
                    doService(curator); // 构建 SingleThreadScheduledExecutor，每隔 3s 检查是否发生时间回调，如果是，直接返回，否则根据当前时间戳构建 data 写入 zookeeper，更新 lastUpdateTime
                    updateLocalWorkerID(workerID);  // 尝试更新本地的 worker id 文件，存在的情况下直接追加，否则创建后在写入内容
                    LOGGER.info("[Old NODE]find forever node have this endpoint ip-{} port-{} workid-{} childnode and start SUCCESS", ip, port, workerID);
                } else {    // 如果没有获取到自己的 worker id
                    //表示新启动的节点,创建持久节点 ,不用check时间
                    String newNode = createNode(curator);   // 创建持久顺序节点 ,并把节点数据放入 value，这里的数据是 buildData 方法构建 EndPoint，持有了本机 ip，zookeeper 端口，时间戳，然后将其转换为 json 串
                    zk_AddressNode = newNode;   // 记录下带 worker id 的全限定路径
                    String[] nodeKey = newNode.split("-");  // 根据全限定路径得到 worker id 串
                    workerID = Integer.parseInt(nodeKey[1]);    // 将 id 串解析为整型
                    doService(curator); // 构建 SingleThreadScheduledExecutor，每隔 3s 检查是否发生时间回调，如果是，直接返回，否则根据当前时间戳构建 data 写入 zookeeper，更新 lastUpdateTime
                    updateLocalWorkerID(workerID);  // 尝试更新本地的 worker id 文件，存在的情况下直接追加，否则创建后在写入内容
                    LOGGER.info("[New NODE]can not find node on forever node that endpoint ip-{} port-{} workid-{},create own node on forever node and start SUCCESS ", ip, port, workerID);
                }
            }
        } catch (Exception e) { // 如果尝试根据 zookeeper 来得到 worker id 失败
            LOGGER.error("Start node ERROR {}", e);
            try {
                Properties properties = new Properties();
                properties.load(new FileInputStream(new File(PROP_PATH.replace("{port}", port + "")))); // 那么尝试读取本地 worker id
                workerID = Integer.valueOf(properties.getProperty("workerID"));
                LOGGER.warn("START FAILED ,use local node file properties workerID-{}", workerID);  // 因为不是重大错误，所以这里警告是使用的本地 worker id
            } catch (Exception e1) {
                LOGGER.error("Read file error ", e1);
                return false;
            }
        }
        return true;
    }
    // 构建 SingleThreadScheduledExecutor，每隔 3s 检查是否发生时间回调，如果是，直接返回，否则根据当前时间戳构建 data 写入 zookeeper，更新 lastUpdateTime
    private void doService(CuratorFramework curator) {
        ScheduledUploadData(curator, zk_AddressNode);// /snowflake_forever/ip:port-000000001
    }
    // 构建 SingleThreadScheduledExecutor，每隔 3s 检查是否发生时间回调，如果是，直接返回，否则根据当前时间戳构建 data 写入 zookeeper，更新 lastUpdateTime
    private void ScheduledUploadData(final CuratorFramework curator, final String zk_AddressNode) {
        Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "schedule-upload-time");  // 这是用于定时上传的线程名字
                thread.setDaemon(true); // 也是守护线程
                return thread;
            }
        }).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                updateNewData(curator, zk_AddressNode); // 检查是否发生时间回调，如果是，直接返回，否则根据当前时间戳构建 data 写入 zookeeper，更新 lastUpdateTime
            }   // 1s 钟开始执行，每隔 3s 上传一次
        }, 1L, 3L, TimeUnit.SECONDS);//每3s上报数据

    }
    // 检查是否发生过时间回拨
    private boolean checkInitTimeStamp(CuratorFramework curator, String zk_AddressNode) throws Exception {
        byte[] bytes = curator.getData().forPath(zk_AddressNode);   // 拿到自己 worker id 对应的数据
        Endpoint endPoint = deBuildData(new String(bytes)); // 差不多就是一个将对象进行反序列化的过程
        //该节点的时间不能小于最后一次上报的时间
        return !(endPoint.getTimestamp() > System.currentTimeMillis()); // 如果返回 true，说明没有发生时间回拨
    }

    /**
     * 创建持久顺序节点 ,并把节点数据放入 value，这里的数据是 buildData 方法构建 EndPoint，持有了本机 ip，zookeeper 端口，时间戳，然后将其转换为 json 串
     *
     * @param curator
     * @return
     * @throws Exception
     */
    private String createNode(CuratorFramework curator) throws Exception {
        try {   // buildData 方法构建 EndPoint，持有了本机 ip，zookeeper 端口，时间戳，然后将其转换为 json 串，因为这里指定了 CreateMode.PERSISTENT_SEQUENTIAL，所以路径是有序列号的
            return curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(PATH_FOREVER + "/" + listenAddress + "-", buildData().getBytes());
        } catch (Exception e) {
            LOGGER.error("create node error msg {} ", e.getMessage());
            throw e;
        }
    }
    // 检查是否发生时间回调，如果是，直接返回，否则根据当前时间戳构建 data 写入 zookeeper，更新 lastUpdateTime
    private void updateNewData(CuratorFramework curator, String path) {
        try {
            if (System.currentTimeMillis() < lastUpdateTime) {  // 这里说明发生了时间回调
                return;
            }   // 将数据信息上传到 zookeeper 中
            curator.setData().forPath(path, buildData().getBytes());
            lastUpdateTime = System.currentTimeMillis();    // 记录下当前的系统事件
        } catch (Exception e) {
            LOGGER.info("update init data error path is {} error is {}", path, e);
        }
    }

    /**
     * 构建需要上传的数据，即构建 EndPoint，持有了本机 ip，zookeeper 端口，时间戳，然后将其转换为 json 串
     *
     * @return
     */
    private String buildData() throws JsonProcessingException {
        Endpoint endpoint = new Endpoint(ip, port, System.currentTimeMillis()); // endpoint 中缓存的是本机 ip 加 zookeeper 的端口号，还有时间戳
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(endpoint);  // 就是将 endpoint 转换为 json 串
        return json;
    }
    // 差不多就是一个将对象进行反序列化的过程
    private Endpoint deBuildData(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Endpoint endpoint = mapper.readValue(json, Endpoint.class); // 反序列化对象
        return endpoint;
    }

    /**
     * 在节点文件系统上缓存一个workid值,zk失效,机器重启时保证能够正常启动
     *
     * @param workerID
     */ // 尝试更新本地的 worker id 文件，存在的情况下直接追加，否则创建后在写入内容
    private void updateLocalWorkerID(int workerID) {
        File LeafconfFile = new File(PROP_PATH.replace("{port}", port));    // 构建 worker id 配置文件
        boolean exists = LeafconfFile.exists(); // 检查该文件是否存在
        LOGGER.info("file exists status is {}", exists);
        if (exists) {
            try {   // 如果文件存在，就向文件中写入 workerID=workerID，非追加的方式
                FileUtils.writeStringToFile(LeafconfFile, "workerID=" + workerID, false);
                LOGGER.info("update file cache workerID is {}", workerID);
            } catch (IOException e) {
                LOGGER.error("update file cache error ", e);
            }
        } else {
            //不存在文件,父目录页肯定不存在
            try {
                boolean mkdirs = LeafconfFile.getParentFile().mkdirs(); // 创建父路径
                LOGGER.info("init local file cache create parent dis status is {}, worker id is {}", mkdirs, workerID);
                if (mkdirs) {   // 如果父路径创建成功
                    if (LeafconfFile.createNewFile()) { // 创建对应的文件
                        FileUtils.writeStringToFile(LeafconfFile, "workerID=" + workerID, false);   // 向文件中追加 workerID=workerID
                        LOGGER.info("local file cache workerID is {}", workerID);
                    }
                } else {
                    LOGGER.warn("create parent dir error===");  // 因为是否有本地缓存文件不影响正常运行，所以这里只是日志警告
                }
            } catch (IOException e) {
                LOGGER.warn("craete workerID conf file error", e);
            }
        }
    }

    private CuratorFramework createWithOptions(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs) {
        return CuratorFrameworkFactory.builder().connectString(connectionString)
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                .build();
    }

    /**
     * 上报数据结构
     */
    static class Endpoint {
        private String ip;
        private String port;
        private long timestamp;

        public Endpoint() {
        }
        // endpoint 中缓存的是本机 ip 加 zookeeper 的端口号，还有时间戳
        public Endpoint(String ip, String port, long timestamp) {
            this.ip = ip;
            this.port = port;
            this.timestamp = timestamp;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }

    public String getZk_AddressNode() {
        return zk_AddressNode;
    }

    public void setZk_AddressNode(String zk_AddressNode) {
        this.zk_AddressNode = zk_AddressNode;
    }

    public String getListenAddress() {
        return listenAddress;
    }

    public void setListenAddress(String listenAddress) {
        this.listenAddress = listenAddress;
    }

    public int getWorkerID() {
        return workerID;
    }

    public void setWorkerID(int workerID) {
        this.workerID = workerID;
    }

}
