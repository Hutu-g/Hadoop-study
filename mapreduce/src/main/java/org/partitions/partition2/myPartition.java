package org.partitions.partition2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class myPartition extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //按号码分区
        String phone = text.toString();
        String subPhone = phone.substring(0, 3);
        int partition;
        if("136".equals(subPhone)){
            partition = 0;
        }else if("137".equals(subPhone)){
            partition = 1;
        }else if("138".equals(subPhone)){
            partition = 2;
        }else if("139".equals(subPhone)){
            partition = 3;
        }else {
            partition = 4;
        }
        return partition;
    }
}
