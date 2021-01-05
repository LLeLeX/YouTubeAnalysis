package com.seu.gulivideo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//继承tool类，查看run源代码发现多了中间解析hadoop参数的代码
public class ETLDriver implements Tool {
    private Configuration configuration;

    public int run(String[] args) throws Exception {
        //1.获取Job对象
        Job job = Job.getInstance(configuration);

        //2.设置jar包路径
        job.setJarByClass(ETLDriver.class);

        //3.设置mapper类输出和输出kv
        job.setMapperClass(ETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //4.设置最终输出的kv
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //5.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6.提交任务
        boolean result = job.waitForCompletion(true);

        return result?0:1;
    }

    public void setConf(Configuration conf) {
        configuration = conf;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {
        //构建配置信息
        Configuration conf = new Configuration();

        try {
            int run = ToolRunner.run(conf, new ETLDriver(), args);
            System.out.println(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
