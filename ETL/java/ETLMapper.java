package com.seu.gulivideo;

import com.seu.util.ETLUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String oriString = value.toString();

        String etlStr = ETLUtil.etlStr(oriString);


        if(etlStr == null){
            return;
        }
        v.set(etlStr);
        context.write(NullWritable.get(), v);
    }
}
