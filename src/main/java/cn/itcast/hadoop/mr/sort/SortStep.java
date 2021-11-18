package cn.itcast.hadoop.mr.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
public class SortStep{
    private static int index = 0;
    private static int tmpnum = 0;


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        if(args.length<2){
            System.err.println("Usage:sorttest <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf,"sort test");
        job.setJarByClass(SortStep.class);
        //job.setJar("sortTest.jar");

        job.setMapperClass(sortMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(sortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(3);

        for(int i=0;i<args.length-1;i++)
        {
            FileInputFormat.addInputPath(job,new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job,new Path(args[args.length-1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class sortMapper extends Mapper<Object,Text,IntWritable,Text>{
        protected void map(Object key,Text value,Context context)throws IOException,InterruptedException{
            int num = Integer.parseInt(value.toString());
            //框架默认是根据键key进行排序，所以先把数字转移到key上面
            context.write(new IntWritable(num),new Text(""));
        }
    }

    public static class sortReducer extends Reducer<IntWritable,Text,Text,Text>{
        protected void reduce(IntWritable key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
            //在key前面在加上一个表示位置的序号
            if(key.get()>tmpnum){
                index++;
                tmpnum = key.get();
            }
            context.write(new Text(index+" "+key.toString()),new Text(""));
        }
    }

}