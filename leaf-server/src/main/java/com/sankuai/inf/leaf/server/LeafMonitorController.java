package com.sankuai.inf.leaf.server;

import com.sankuai.inf.leaf.segment.SegmentIDGenImpl;
import com.sankuai.inf.leaf.server.model.SegmentBufferView;
import com.sankuai.inf.leaf.segment.model.LeafAlloc;
import com.sankuai.inf.leaf.segment.model.SegmentBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
public class LeafMonitorController {
    private Logger logger = LoggerFactory.getLogger(LeafMonitorController.class);
    @Autowired
    SegmentService segmentService;

    @RequestMapping(value = "cache")
    public String getCache(Model model) {
        Map<String, SegmentBufferView> data = new HashMap<>();
        SegmentIDGenImpl segmentIDGen = segmentService.getIdGen();  // 获取 SegmentIDGenImpl，监视器只是对 buffer 的 segment 的数据内容进行监控
        if (segmentIDGen == null) {
            throw new IllegalArgumentException("You should config leaf.segment.enable=true first");
        }
        Map<String, SegmentBuffer> cache = segmentIDGen.getCache(); // 获取 idGen 持有的 cache，cache 中缓存的是 biz-tag -> buffer（持有两个 segment）
        for (Map.Entry<String, SegmentBuffer> entry : cache.entrySet()) {
            SegmentBufferView sv = new SegmentBufferView(); // 构建 buffer segment 的视图对象，可以理解为 pojo
            SegmentBuffer buffer = entry.getValue();    // 得到 biz-tag 对应的 buffer
            sv.setInitOk(buffer.isInitOk());    // 将 buffer 的 init 状态填充到 SegmentBufferView 中
            sv.setKey(buffer.getKey()); // 将 buffer 的 biz-tag 状态填充到 SegmentBufferView 中
            sv.setPos(buffer.getCurrentPos());  // 获取当前使用的 segment 对应的 pos
            sv.setNextReady(buffer.isNextReady());  // 获取另外一个未被使用的 segment 的就绪信息
            sv.setMax0(buffer.getSegments()[0].getMax());   // 这里就是读取 segment[0] 的最大 id 值进行填充
            sv.setValue0(buffer.getSegments()[0].getValue().get()); // 这里就是读取 segment[0] 的当前 id 值进行填充
            sv.setStep0(buffer.getSegments()[0].getStep()); // 这里就是读取 segment[0] 的 step 步长值进行填充

            sv.setMax1(buffer.getSegments()[1].getMax());   // 跟上边同理
            sv.setValue1(buffer.getSegments()[1].getValue().get());
            sv.setStep1(buffer.getSegments()[1].getStep());
            // 一个 biz-tag 对应一个 SegmentBufferView，添加到 data map 集合中
            data.put(entry.getKey(), sv);

        }
        logger.info("Cache info {}", data); // 日志记录缓存信息
        model.addAttribute("data", data);   // 将数据添加到 model 中
        return "segment";   // 返回视图
    }

    @RequestMapping(value = "db")
    public String getDb(Model model) {
        SegmentIDGenImpl segmentIDGen = segmentService.getIdGen();  // 获取 SegmentIDGenImpl，监视器只是对 buffer 的 segment 的数据内容进行监控
        if (segmentIDGen == null) {
            throw new IllegalArgumentException("You should config leaf.segment.enable=true first");
        }   // 就是从数据库中查询全部记录的指定四个字段，结果通过 List<LeafAlloc> 承载返回
        List<LeafAlloc> items = segmentIDGen.getAllLeafAllocs();
        logger.info("DB info {}", items);
        model.addAttribute("items", items); // 返回视图
        return "db";
    }

}
