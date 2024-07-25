package org.flowbeans.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class JoinReducer extends Reducer<Text, JoinBean,JoinBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<JoinBean> values, Reducer<Text, JoinBean, JoinBean, NullWritable>.Context context) throws IOException, InterruptedException {

        ArrayList<JoinBean> orderList = new ArrayList<JoinBean>();
        JoinBean pdBean = new JoinBean();
        for (JoinBean value : values) {
            if ("order".equals(value.getFlag())){
                JoinBean temp = new JoinBean();
                try {
                    BeanUtils.copyProperties(temp,value);
                    orderList.add(temp);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }else {
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        for (JoinBean joinBean : orderList) {
            joinBean.setPname(pdBean.getPname());
            context.write(joinBean,NullWritable.get());
        }

    }
}
