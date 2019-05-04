package com.lichzhang.flinkTest;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

    private final int topSize;
    private ListState<ItemViewCount> itemState;

    public TopNHotItems(int topSize) {

        this.topSize = topSize;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ListStateDescriptor<ItemViewCount> itemsStateDesc =
                new ListStateDescriptor<ItemViewCount>(
                        "itemStae-state",
                        ItemViewCount.class
                );
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }


    @Override
    public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws  Exception{
        itemState.add(itemViewCount);
        context.timerService().registerEventTimeTimer(itemViewCount.windowEnd + 1);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        //super.onTimer(timestamp, ctx, out);

        //获取所有商品的点击量
        List<ItemViewCount> allItems = new ArrayList<>();
        for (ItemViewCount item : itemState.get()) {
            allItems.add(item);
        }

        //提前清楚状态中的数据，释放空间
        itemState.clear();

        //排序
        allItems.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int) (o2.viewCount - o1.viewCount);
            }
        });

        StringBuilder result = new StringBuilder();
        result.append("============================\n");
        result.append("时间：").append(new Timestamp(timestamp -1)).append("\n");
        for (int i = 0 ; i < topSize; i ++) {
            ItemViewCount currentItem = allItems.get(i);

            result.append("No").append(i).append(":")
                    .append("   商品ID=").append(currentItem.itemId)
                    .append("   浏览量=").append(currentItem.viewCount)
                    .append("\n");
        }
        result.append("============================\n\n");


        out.collect(result.toString());
    }
}

