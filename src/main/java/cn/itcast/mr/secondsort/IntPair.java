package cn.itcast.mr.secondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {
    int first;
    int second;

    /**
     * Set the left and right values.
     */
    public void set(int left, int right) {
        first = left;
        second = right;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    // 反序列化，从流中的二进制转换成IntPair
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    // 序列化，将IntPair转化成使用流传送的二进制
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    // key的比较
    public int compareTo(IntPair o) {
        // TODO Auto-generated method stub
        if (first != o.first) {
            return first < o.first ? -1 : 1;
        } else if (second != o.second) {
            return second < o.second ? -1 : 1;
        } else {
            return 0;
        }
    }


}
