package org.partitionAndComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @Author: hutu
 * @Date: 2024/7/21 21:02
 */
public class myPartition extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
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
