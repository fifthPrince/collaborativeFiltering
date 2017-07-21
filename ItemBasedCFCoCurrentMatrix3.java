import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

public class ItemBasedCFCoCurrentMatrix3 {

	public static class Map1 extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
		
		private Text internalKey = new Text();
		private Text internalValue = new Text("1");
		private String flag;

		
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			
			String[] splitted = value.toString().split("\\s");
			String[] moviepair = splitted[0].split(",");
			internalKey.set(moviepair[0]);
			internalValue.set('A'+moviepair[1]+":"+splitted[1]);
			output.collect(internalKey, internalValue);		
			
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
			internalKey.set(splitted[1]);
			internalValue.set('B'+splitted[0]+":"+splitted[2]);
			output.collect(internalKey, internalValue);		
			
		}
	}

	public static class Reduce extends MapReduceBase implements
	Reducer<Text, Text, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
						throws IOException {
			
			DoubleWritable outputValue = new DoubleWritable();
			Text outputKey = new Text();
			Map movieCocurrentMap = new HashMap();
			Map userScoreMap = new HashMap();
			

			int sum = 0;
			while(values.hasNext()){
				String line = values.next().toString();
				if(line.startsWith("A")){
					String item = line.substring(1);
					String[] splitted = item.split(":");
					movieCocurrentMap.put(splitted[0], splitted[1]);
				}else{
					String item = line.substring(1);
					String[] splitted = item.split(":");
					userScoreMap.put(splitted[0], splitted[1]);
				}
			}
			
			Iterator iteruser = userScoreMap.entrySet().iterator();
			while(iteruser.hasNext()){
				Map.Entry entry = (Map.Entry )iteruser.next();
				String keyuser = (String)entry.getKey();
				String valueuser = (String)entry.getValue();
				Iterator itermovie = movieCocurrentMap.entrySet().iterator();
				while(itermovie.hasNext()){
					Map.Entry entry2 = (Map.Entry)itermovie.next();
					String keymovie = (String)entry2.getKey();
					String valuemovie = (String)entry2.getValue();
					outputKey.set(key + ":" + keyuser+","+keymovie);
					outputValue.set(Double.parseDouble(valueuser)*Double.parseDouble(valuemovie));
					output.collect(outputKey, outputValue);
				}
			}			
			
			
			
		}	

		
	}
	

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ItemBasedCFCoCurrentMatrix3.class);
		conf.setJobName("ItemBasedCFCoCurrentMatrix3");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		//conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		//FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.0.2:9000/sparkDir//cocurrentmatrix/2.out/part-00000"));
		//FileInputFormat.addInputPath(conf, new Path("hdfs://192.168.0.2:9000/sparkDir/smallest"));
		MultipleInputs.addInputPath(conf, new Path("hdfs://192.168.0.2:9000/sparkDir//cocurrentmatrix/2.out/part-00000"), TextInputFormat.class,Map1.class);
		MultipleInputs.addInputPath(conf, new Path("hdfs://192.168.0.2:9000/sparkDir/smallest2"), TextInputFormat.class,Map2.class);
		
		//FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.0.2:9000/sparkDir/smallest"));
		FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.0.2:9000/sparkDir/cocurrentmatrix/3.out"));
	
		JobClient.runJob(conf);
	}

}
