import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
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

public class ItemBasedCFCoCurrentMatrix2 {

	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
		
		private Text internalKey = new Text();
		private Text internalValue = new Text("1");
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			
			String[] splitted = value.toString().split(",");
			for(int i = 0; i < splitted.length; i++){
				String[] movieRatingPair = splitted[i].split(":");
				for(int j = i; j < splitted.length; j++){
					String[] movieRatingPair2 = splitted[j].split(":");
					String first = movieRatingPair2[0];
					String second = movieRatingPair[0];
					if(Integer.parseInt(movieRatingPair[0]) < Integer.parseInt(movieRatingPair2[0]))
					{
						first = movieRatingPair[0];
						second = movieRatingPair2[0];
					}
					String result = first + "," + second;
					internalKey.set(result);
					output.collect(internalKey,internalValue);
				}
			}					
		}
	}

	public static class Reduce extends MapReduceBase implements
	Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
						throws IOException {
			
			Text outputValue = new Text();

			int sum = 0;
			while(values.hasNext()){
				values.next();
				sum++;
			}
			
			output.collect(key, new IntWritable(sum));
			String[] keypair = key.toString().split(",");
			if(!keypair[0].equals(keypair[1])){
			  Text key2 = new Text(keypair[1]+","+keypair[0]);
			  output.collect(key2, new IntWritable(sum));
			}
			
			
		}	

		
	}
	

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ItemBasedCFCoCurrentMatrix2.class);
		conf.setJobName("ItemBasedCFCoCurrentMatrix2");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.0.2:9000/sparkDir//cocurrentmatrix/1.out/part-00000"));
		//FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.0.2:9000/sparkDir/smallest"));
		FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.0.2:9000/sparkDir/cocurrentmatrix/2.out"));


		JobClient.runJob(conf);
	}

}
