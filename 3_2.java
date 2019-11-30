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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Edward on 2016/7/22.
 */
public class Step2 {
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

            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            
            job.addCacheFile(new Path("/test/tfidf/output/part-r-00002").toUri());
                
            job.addCacheFile(new Path("/test/tfidf/output/part-r-00003").toUri());

            FileInputFormat.addInputPath(job, new Path("/test/tfidf/output"));

            Path path = new Path("/test/tfidf/output1");
            if(fs.exists(path))
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

        public static Map<String, Double> dfmap = new HashMap<String, Double>();

        public static Map<String, Double> totalmap = new HashMap<String, Double>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path pArtNum = new Path(cacheFiles[0].getPath());
            Path pArtTotal = new Path(cacheFiles[1].getPath());

            
            BufferedReader buffer = new BufferedReader(new FileReader(pArtNum.getName()));
            String line = null;
            while((line = buffer.readLine()) != null){
                String[] str = line.split("\t");
                dfmap.put(str[0], Double.parseDouble(str[1]));
            }

            
            buffer = new BufferedReader(new FileReader(pArtTotal.getName()));
            line = null;
            while((line = buffer.readLine()) != null){
                String[] str = line.split("\t");
                totalmap.put(str[0], Double.parseDouble(str[1]));
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] strings = value.toString().split("\t");
            String k = strings[0];

            if(k.contains("counter")) {
                
            }
            else if(k.contains("_")){
                String word = k.split("_")[0];
                String[] info = strings[1].split(",");
                String n=null;
                String num=null;
                if(info[0].contains("A")){
                    n = info[0].substring(info[0].indexOf(":")+1);
                    num = info[1].substring(info[0].indexOf(":")+1);
                }
                if(info[0].contains("B")){
                    num = info[0].substring(info[0].indexOf(":")+1);
                    n = info[1].substring(info[0].indexOf(":")+1);
                }
                double result = 0l;

                result = (Double.parseDouble(n)/Double.parseDouble(num)) * Math.log( totalmap.get("counter")/dfmap.get(word));
                System.out.println("n=" + Double.parseDouble(n));
                System.out.println("num=" + Double.parseDouble(num));
                System.out.println("counter=" + totalmap.get("counter"));
                System.out.println("wordnum=" + dfmap.get(word));
                context.write(new Text(k.split("_")[1]), new Text(word+":"+result));
            }
            else{
               
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder stringBuilder = new StringBuilder();
            for(Text t: values){
                stringBuilder.append(t.toString());
                stringBuilder.append("\t");
            }
            context.write(key, new Text(stringBuilder.toString()) );
        }
    }
}
