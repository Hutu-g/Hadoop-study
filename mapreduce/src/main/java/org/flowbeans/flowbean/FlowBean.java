package org.flowbeans.flowbean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * @Author: hutu
 * @Date: 2024/7/15 16:37
 */
public class FlowBean  implements Writable {
    public FlowBean() {
    }
    private int up;
    private int down;
    private int sum;

    public int getUp() {
        return up;
    }

    public void setUp(int up) {
        this.up = up;
    }
    public int getDown() {
        return down;
    }
    public FlowBean(int up, int down, int sum) {
        this.up = up;
        this.down = down;
        this.sum = sum;
    }

    public void setDown(int down) {
        this.down = down;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(up);
        dataOutput.writeInt(down);
        dataOutput.writeInt(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.up = dataInput.readInt();
        this.down = dataInput.readInt();
        this.sum = dataInput.readInt();
    }

    @Override
    public String toString() {
        return  up +
                "\t" + down +
                "\t" + sum;
    }
}
