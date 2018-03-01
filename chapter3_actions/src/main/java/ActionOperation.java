import scala.Tuple2;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

public class ActionOperation
{
    public static void reduce() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("reduce");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        // reduce - here is where we place our action
        Integer reduce = listRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)
                    throws Exception
            {
                return integer + integer2;
            }
        });
        System.out.println(reduce);
    }

    // collect
    public static void collect() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("collect");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista = Arrays.asList(1, 2, 3, 4);
        List<Integer> listb = Arrays.asList(3, 2, 3, 9);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<Integer> listbRDD = sc.parallelize(listb);
        JavaRDD<Integer> union = listaRDD.union(listbRDD);
        List<Integer> collect = union.collect();
	System.out.println(Arrays.toString(collect.toArray()));
    }

    // topN
    public static void take() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("take");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        List<Integer> take =  listRDD.take(3);
	System.out.println(Arrays.toString(take.toArray()));
    }

    public static void count() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        long count = listRDD.count();
        System.out.println(count);
    }

    // topN - Ordered
    public static void takeOrdered() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("takeOrdered");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 222, 3, 40, 500, 6, 71, 18, 9, 100);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        // Here we will get 3 elements ordered
        List<Integer> takeOrdered = listRDD.takeOrdered(3);
        System.out.println(Arrays.toString(takeOrdered.toArray()));
        List<Integer> top = listRDD.top(3);
	System.out.println(Arrays.toString(top.toArray()));
    }

    public static void saveAsTextFile() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("saveAsTextFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista = Arrays.asList(1, 2, 3, 4);
        List<Integer> listb = Arrays.asList(3, 2, 3, 9);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<Integer> listbRDD = sc.parallelize(listb);
        JavaRDD<Integer> union = listaRDD.union(listbRDD);
	//Saving to file
        union.repartition(1).saveAsTextFile("/union");
    }

    public static void countByKey() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("countByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("Joe", 90),
                new Tuple2<String, Integer>("Nick", 100),
                new Tuple2<String, Integer>("Joe", 90),
                new Tuple2<String, Integer>("Nick", 100),
                new Tuple2<String, Integer>("Joe", 90),
                new Tuple2<String, Integer>("Nick", 70),
                new Tuple2<String, Integer>("Don", 60),
                new Tuple2<String, Integer>("Don", 90),
                new Tuple2<String, Integer>("Don", 100));
        JavaPairRDD<String, Integer> listRDD = sc.parallelizePairs(list);
        Map<String, Long> countByKey = listRDD.countByKey();
        for (String key : countByKey.keySet()) {
            System.out.println(key + ": " + countByKey.get(key));
        }
    }

    public static void takeSample() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("takeSample");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 222, 3, 40, 500, 6, 71, 18, 9, 100);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
	//We are taking two
        List<Integer> takeSample = listRDD.takeSample(true, 2);
        for (Integer sample : takeSample) {
            System.out.println(sample);
        }
    }

    public static void main(String[] args)
    {
        String exampleToRun = args[0];

        switch (exampleToRun) {
         case "reduce":
             reduce();
             break;
         case "collect":
             collect();
             break;
         case "take":
             take();
             break;
	 case "count":
	     count();
	     break;
         case "takeOrdered":
             takeOrdered();
             break;
         case "saveAsTextFile":
             saveAsTextFile();
             break;
         case "countByKey":
             countByKey();
             break;
         default:
             throw new IllegalArgumentException("Invalid Example selected ");
        }
    }
}
