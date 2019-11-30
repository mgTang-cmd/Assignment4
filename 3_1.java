import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by Edward on 2016/7/21.
 */
public class Step1 {

    public static void main(String[] args)
    {
        //access hdfs's user
        //System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");

        try {
            FileSystem fs = FileSystem.get(conf);

            Job job = Job.getInstance(conf);
            job.setJarByClass(RunJob.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);
            job.setPartitionerClass(FilterPartition.class);


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            job.setNumReduceTasks(4);

            FileInputFormat.addInputPath(job, new Path("/test/tfidf/input"));

            Path path = new Path("/test/tfidf/output");
            if(fs.exists(path))//如果目录存在，则删除目录
            {
                fs.delete(path,true);
            }
            FileOutputFormat.setOutputPath(job, path);

            boolean b = job.waitForCompletion(true);
            if(b)
            {
                System.out.println("OK");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text > {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<String, Integer>();

            String[] str = value.toString().split("\t");
            StringReader stringReader = new StringReader(str[1]);
            IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
            Lexeme lexeme = null;
            Long count = 0l;
            while((lexeme = ikSegmenter.next())!=null) {
                String word = lexeme.getLexemeText();
                if(map.containsKey(word)) {
                    map.put(word, map.get(word)+1);
                }
                else{
                    map.put(word, 1);
                }
                count++;
            }
            for(Entry<String, Integer> entry: map.entrySet())
            {
                context.write(new Text(entry.getKey()+"_"+str[0]), new Text("A:"+entry.getValue()));
                context.write(new Text(entry.getKey()+"_"+str[0]), new Text("B:"+count));
                context.write(new Text(entry.getKey()),new Text("1"));
            }
            context.write(new Text("counter"), new Text(1+""));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            if(key.toString().equals("conter")) {
                long sum = 0l;
                for(Text v :values)
                {
                    sum += Long.parseLong(v.toString());
                }
                context.write(key, new Text(sum+""));
            }
            else{
                if(key.toString().contains("_")) {
                    StringBuilder stringBuilder = new StringBuilder();
                    for (Text v : values) {
                        stringBuilder.append(v.toString());
                        stringBuilder.append(",");
                    }
                    context.write(key, new Text(stringBuilder.toString()));
                }
                else {
                    long sum = 0l;
                    for(Text v :values)
                    {
                        sum += Long.parseLong(v.toString());
                    }
                    context.write(key, new Text(sum+""));
                }
            }
        }
