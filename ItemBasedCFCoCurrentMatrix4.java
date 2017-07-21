import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class ItemBasedCFCoCurrentMatrix4 {

	public static class Map1 extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
		
		private Text internalKey = new Text();
		private Text internalValue = new Text("1");
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			
			String[] splitted = value.toString().split("\\s+");
			String[] scores = splitted[0].split(":");
			internalValue.set(splitted[1]);
			internalKey.set(scores[1]);
			output.collect(internalKey,internalValue);					
		}
	}
	
	public static class Map2 extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
		
		private Text internalKey = new Text();
		private Text internalValue = new Text("1");
		private String flag;

		
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {		

			
			String[] splitted = value.toString().split("\\s+");			
			internalKey.set(splitted[0] +"," + splitted[1]);
			internalValue.set("B");
			output.collect(internalKey, internalValue);		
			
		}
	}

	public static class Reduce extends MapReduceBase implements
	Reducer<Text, Text, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
						throws IOException {
			
			Text outputValue = new Text();

			double sum = 0;
			double score = 0;
			while(values.hasNext()){
				String line = values.next().toString();
				if(line.equals("B")){
					output.collect(key, new DoubleWritable(0));
					return;
				}
				score = Double.parseDouble(line);
				sum+=score;
			}
			
			output.collect(key, new DoubleWritable(sum));		
			
		}	

		
	}
	

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ItemBasedCFCoCurrentMatrix4.class);
		conf.setJobName("ItemBasedCFCoCurrentMatrix4");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		//conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		
		MultipleInputs.addInputPath(conf, new Path("hdfs://192.168.0.2:9000/sparkDir//cocurrentmatrix/3.out/part-00000"), TextInputFormat.class,Map1.class);
		MultipleInputs.addInputPath(conf, new Path("hdfs://192.168.0.2:9000/sparkDir/smallest2"), TextInputFormat.class,Map2.class);
		FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.0.2:9000/sparkDir/cocurrentmatrix/4.out"));


		JobClient.runJob(conf);
	}

}
