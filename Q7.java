
import java.io.IOException;
//import java.util.Map;
//import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q7 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cfg=new Configuration();
		
		Job job=Job.getInstance(cfg,"Top 5 Job Title");
		
		job.setJarByClass(Q7.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}

class MyMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
	
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String applications=value.toString().split("\t")[0];
		String apb[] = value.toString().split("\t");
		int year = Integer.parseInt(apb[7]);
		context.write(new IntWritable(year), new Text(applications));
	}
}

class MyReducer extends Reducer<IntWritable,Text,Text,IntWritable>{
	TreeMap<Integer,String> tm=new TreeMap<Integer,String>();
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
		int total_count=0;
		for(IntWritable val:values){
			total_count= total_count + val.get();
		}
		
		tm.put(total_count, key.toString());
		
		
		
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		for(Integer key:tm.keySet()){
			context.write(new Text(tm.get(key)), new IntWritable(key));
		}
		
	}
}