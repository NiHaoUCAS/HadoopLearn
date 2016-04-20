import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class SubjectScore extends Configured implements Tool{
	public static class Map	extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line= value.toString();
			System.out.println(key.toString());
			System.out.println(line);
			StringTokenizer tokenArticle= new StringTokenizer(line,"\n");
			while(tokenArticle.hasMoreTokens()){
				StringTokenizer tokenizerLine= new StringTokenizer(tokenArticle.nextToken());
				String strName = tokenizerLine.nextToken();
				String strScore = tokenizerLine.nextToken();

				Text name= new Text(strName);
				int scoreInt= Integer.parseInt(strScore);
				context.write(name, new IntWritable(scoreInt));
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
			int sum=0;
			int count=0;
			Iterator<IntWritable> iterator =value.iterator();
			while(iterator.hasNext()){
				sum+=iterator.next().get();
				count++;
			}
			int average= (int) sum/count;
			context.write(key, new IntWritable(average));
		}
	}

	public int run(String [] args) throws Exception{
		Job job =new Job(getConf());
		job.setJarByClass(SubjectScore.class);
		job.setJobName("SubjectScore");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;			
	}

	public static void main(String [] args) throws Exception{
		int ret=ToolRunner.run(new SubjectScore(), args);
		System.exit(ret);
	}
}


