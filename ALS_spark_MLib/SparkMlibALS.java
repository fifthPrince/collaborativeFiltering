package duwei.spark.Mlibs;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public class SparkMlibALS {
	
	
  static class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
		    @Override
		    public String call(Tuple2<Object, double[]> element) {
		      return element._1() + "," + Arrays.toString(element._2());
		    }
		  }

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setMaster("local").setAppName("spark001");
		//SparkConf conf = new SparkConf().setMaster("spark://192.168.0.2:7077").setAppName("spark001");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		//jsc.addJar("/hadoop/eclipseNeon26/eclipseNeon26/sparkMlibJava.jar");
		
		double starttime = System.currentTimeMillis();
		
		JavaRDD<String> lines = jsc.textFile("hdfs://192.168.0.2:9000/sparkDir/ua.base");
		
		
		JavaRDD<String> lines2 = lines.filter(line -> !line.contains("userId"));
		
		JavaRDD<Rating> rates = lines2.map(new Function<String, Rating>(){

			@Override
			public Rating call(String s)  {				
				
				Rating r = null;
				
				
				try{
				
				String[] sarray = s.split("\\s+");
				
				int i = Integer.parseInt(sarray[0]);
				
				r =  new Rating(i,Integer.parseInt(sarray[1]),Double.parseDouble(sarray[2]));
				}catch(Exception e){
					r =  new Rating(0,0,0);
				}
				
				return r;
			}
			
		});
		
		//System.out.println(rates.take(10).toString());
		
			
		MatrixFactorizationModel  model = ALS.train(rates.rdd(),2, 2,0.01);
		
		JavaRDD<Tuple2<Object, Object>> userProducts = rates.map(r->new Tuple2<>(r.user(),r.product()));
		
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
				  model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
				      .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
				);
		
		JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(
			    rates.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
			  .join(predictions).values();
		
		double MSE = ratesAndPreds.mapToDouble(pair -> {
			  double err = pair._1() - pair._2();
			  return err * err;
			}).mean();
			System.out.println("Mean Squared Error = " + MSE);
		
//		model.userFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(
//		        "userFeatures");
//		    model.productFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(
//		        "productFeatures");
//		    
//		    JavaRDD<Tuple2<Object, double[]>> userfeature = model.userFeatures().toJavaRDD();
		
		    double endtime = System.currentTimeMillis();
		    System.out.println("total time is " + (endtime - starttime)/1000);

	}

}
