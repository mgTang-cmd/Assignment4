import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class FilterPartition extends HashPartitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {

        if(key.toString().contains("counter"))
        {
            return numReduceTasks-1;
        }

        if(key.toString().contains("_"))
        {
            return super.getPartition(key, value, numReduceTasks-2);
        }
        else
        {
            return numReduceTasks-2;
        }
    }
}
