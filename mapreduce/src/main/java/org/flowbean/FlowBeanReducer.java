package org.flowbean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @Author: hutu
 * @Date: 2024/7/15 16:37
 */
public class FlowBeanReducer extends Reducer<Text, FlowBean,Text, FlowBean> {
    private FlowBean flowBean =new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        int upSum = 0;
        int downSum = 0;
        for (FlowBean value : values) {
            upSum += value.getUp();
            downSum += value.getDown();
            flowBean.setSum(value.getSum());
        }

        flowBean.setUp(upSum);
        flowBean.setDown(downSum);

        context.write(key,flowBean);
    }
}
