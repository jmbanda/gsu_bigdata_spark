import java.util.Arrays; 
import java.util.Iterator; 
import java.util.List; 

import org.apache.spark.SparkConf; 
import org.apache.spark.api.java.JavaPairRDD; 
import org.apache.spark.api.java.JavaRDD; 
import org.apache.spark.api.java.JavaSparkContext; 
import org.apache.spark.api.java.function.FlatMapFunction; 
import org.apache.spark.api.java.function.Function; 
import org.apache.spark.api.java.function.Function2; 
import org.apache.spark.api.java.function.VoidFunction; 

import scala.Tuple2; 

@SuppressWarnings(value = {"unused", "unchecked"}) 
public class TransformationOperation { 

    public static void main(String[] args) {

	String exampleToRun = args[0];

        switch (exampleToRun) {
         case "map":
             map();
             break;
         case "filter":
	     filter();
	     break;
         case "flatMap":
	     flatMap();
	     break;
	 case "groupByKey":
	     groupByKey();
             break;
         case "reduceByKey":
	     reduceByKey();
	     break;
         case "sortByKey":
	     sortByKey();
	     break;
         case "join":
	     join();
             break;
         case "cogroup":
	     cogroup();
             break;			 
         default:
             throw new IllegalArgumentException("Invalid Example selected ");
        }
        // map();
        // filter(); 
        // flatMap(); 
        // groupByKey(); 
        // reduceByKey(); 
        // sortByKey(); 
        // join(); 
        // cogroup();
    } 

    /** 
     * The case of map operator: Multiply every element in the set by 2. 
     */ 
    private static void map() { 
        // Create SparkConf. 
        SparkConf conf = new SparkConf() 
                .setAppName("map") 
                .setMaster("local"); 
        // Create JavaSparkContext. 
        JavaSparkContext sc = new JavaSparkContext(conf); 

        // Construct a set. 
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5); 

        // Parallelize sets and create the initial RDD. 
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers); 

        // Use the map operator to multiply every element in the set by 2. 
        // The map operator can call any type of RDD. 
        // In Java, parameters that a map operator receives are Function objects. 
        // The created Function object is sure to request you to set the second generic parameter. This generic type is the type of the returned new element. 
            // At the same time, the returned type of the call() method also must be synchronized with the second generic type. 
        // Within the call() method, we can perform various processing and computation on every element in the original RDD, and return a new element. 
        // All the new elements will form a new RDD. 
        JavaRDD<Integer> multipleNumberRDD = numberRDD.map( 

                new Function<Integer, Integer>() { 

                    private static final long serialVersionUID = 1L; 

                    // The elements passed to the call() method are 1, 2, 3, 4, 5. 
                    // The returned values are 2, 4, 6, 8, 10. 
                    @Override 
                    public Integer call(Integer v1) throws Exception { 
                        return v1 * 2; 
                    } 

                }); 

        // Print a new RDD. 
        multipleNumberRDD.foreach(new VoidFunction<Integer>() { 

            private static final long serialVersionUID = 1L; 

            @Override 
            public void call(Integer t) throws Exception { 
                System.out.println(t);   
            } 

        }); 

        // Disable JavaSparkContext. 
        sc.close(); 
    } 

    /** 
     * The case of the filter operator: Filter the even number in the set. 
     */ 
    private static void filter() { 
        // Create SparkConf. 
        SparkConf conf = new SparkConf() 
                .setAppName("filter") 
                .setMaster("local"); 
        // Create JavaSparkContext. 
        JavaSparkContext sc = new JavaSparkContext(conf); 

        // Simulate a set. 
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10); 

        // Parallelize sets and create the initial RDD. 
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers); 

        // Execute the filter operator to the initial RDD to filter the even numbers in it. 
        // The parameters passed into the filter operator are also Function objects. The filter operator shares the same points of attention for usage with the map operator. 
        // However, they are different only in that the returned type of the call() method for the filter operator is Boolean. 
        // Every element in the initial RDD will be passed into the call() method. At this time, you can execute various self-defined computation logic 
        // to determine whether this element is what you want. 
        // If you want to keep this element in the new RDD, you should return true; otherwise, you should return false. 
        JavaRDD<Integer> evenNumberRDD = numberRDD.filter( 

                new Function<Integer, Boolean>() { 

                    private static final long serialVersionUID = 1L; 

                    // Here, all the numbers from 1 to 10 will be passed in. 
                    // But according to our logic, only the even numbers, namely 2, 4, 6, 8, 10, will return true. 
                    // So only the even numbers are reserved in the new RDD. 
                    @Override 
                    public Boolean call(Integer v1) throws Exception { 
                        return v1 % 2 == 0; 
                    } 

                }); 

        // Print a new RDD. 
        evenNumberRDD.foreach(new VoidFunction<Integer>() { 

            private static final long serialVersionUID = 1L; 

            @Override 
            public void call(Integer t) throws Exception { 
                System.out.println(t); 
            } 

        }); 

        // Disable JavaSparkContext. 
        sc.close(); 
    } 

    /** 
     * The case of flatMap: Split a text line into multiple words. 
     */ 
    private static void flatMap() { 
        // Create SparkConf. 
        SparkConf conf = new SparkConf() 
                .setAppName("flatMap")   
                .setMaster("local");   
        // Create JavaSparkContext. 
        JavaSparkContext sc = new JavaSparkContext(conf); 

        // Construct a set. 
        List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");   

        // Parallelize sets and create the RDD. 
        JavaRDD<String> lines = sc.parallelize(lineList); 

        // Execute the flatMap operator on the RDD to split every text line into multiple words. 
        // In Java, the flatMap operator receives FlatMapFunction parameters. 
        // We need to define the second generic type of FlatMapFunction, that is, the type of the returned new element. 
        // The returned type of the call() method is not U, but Iterable<U>. Here the U is also consistent with the second generic type. 
        // The flatMap operator is actually to receive every element from the original RDD and perform computation and processing with various logic. It can return multiple elements. 
        // The multiple elements are encapsulated in the Iterable set. You can also use the ArrayList set and others. 
        // All the new elements are encapsulated in the new RDD; that is, the size of the new RDD must be bigger than or equal to the size of the original RDD. 
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() { 

            private static final long serialVersionUID = 1L; 

            // Here the first line will be passed, for example, hello you. 
            // The Iterable<String>(hello, you) is returned. 
            @Override 
            public Iterator<String> call(String t) throws Exception { 
                return Arrays.asList(t.split(" ")).iterator(); 
            } 

        }); 

        // Print a new RDD. 
        words.foreach(new VoidFunction<String>() { 

            private static final long serialVersionUID = 1L; 

            @Override 
            public void call(String t) throws Exception { 
                System.out.println(t); 
            } 
        }); 

        // Disable JavaSparkContext. 
        sc.close(); 
    } 

    /** 
     * The case of groupByKey: Group the scores by class. 
     */ 
    private static void groupByKey() { 
        // Create SparkConf. 
        SparkConf conf = new SparkConf() 
                .setAppName("groupByKey")   
                .setMaster("local"); 
        // Create JavaSparkContext. 
        JavaSparkContext sc = new JavaSparkContext(conf); 

        // Simulate a set. 
        List<Tuple2<String, Integer>> scoreList = Arrays.asList( 
                new Tuple2<String, Integer>("class1", 80), 
                new Tuple2<String, Integer>("class2", 75), 
                new Tuple2<String, Integer>("class1", 90), 
                new Tuple2<String, Integer>("class2", 65)); 

        // Parallelize sets and create the JavaPairRDD. 
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList); 

        // Execute the groupByKey operator on the scores RDD to group the scores of every class. 
        // The groupByKey operator still returns JavaPairRDD. 
        // However, the first generic type of JavaPairRDD remains the same, and the second generic type is changed to the Iterable set type. 
        // That is to say, by grouping the scores by the key, every key may have multiple values. The multiple values are aggregated into the Iterable set. 
        // So next, we can just conveniently process the data in a group through the JavaPairRDD such as groupedScores. 
        JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey(); 

        // Print groupedScores RDD. 
        groupedScores.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() { 

            private static final long serialVersionUID = 1L; 

            @Override 
            public void call(Tuple2<String, Iterable<Integer>> t) 
                    throws Exception { 
                System.out.println("class: " + t._1);   
                Iterator<Integer> ite = t._2.iterator(); 
                while(ite.hasNext()) { 
                    System.out.println(ite.next());   
                } 
                System.out.println("==============================");   
            } 

        }); 

        // Disable JavaSparkContext. 
        sc.close(); 
    } 

    /** 
     * The case of reduceByKey: Calculate the total score of every class. 
     */ 
    private static void reduceByKey() { 
        // Create SparkConf. 
        SparkConf conf = new SparkConf() 
                .setAppName("reduceByKey")   
                .setMaster("local"); 
        // Create JavaSparkContext. 
        JavaSparkContext sc = new JavaSparkContext(conf); 

        // Simulate a set. 
        List<Tuple2<String, Integer>> scoreList = Arrays.asList( 
                new Tuple2<String, Integer>("class1", 80), 
                new Tuple2<String, Integer>("class2", 75), 
                new Tuple2<String, Integer>("class1", 90), 
                new Tuple2<String, Integer>("class2", 65)); 

        // Parallelize sets and create the JavaPairRDD. 
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList); 

        // Execute the reduceByKey operator on scores RDD. 
        // The reduceByKey operator receives parameters of the Function2 type. It has three generic parameters representing three values. 
        // The first generic type and the second generic type represent the value type of the elements in the original RDD. 
            // So every key will be reduced. The first and second values will be passed in succession first, and their returned value will be passed together with the third value. 
            // So here two generic types will be defined automatically, indicating the types of the two incoming parameters of the call() method. 
        // The third generic type represents the type of the returned value of every reduce operation. By default, it is also of the same type with the value type of the original RDD. 
        // The RDD returned by the reduceByKey operator is still JavaPairRDD<key, value>. 
        JavaPairRDD<String, Integer> totalScores = scores.reduceByKey( 

                new Function2<Integer, Integer, Integer>() { 

                    private static final long serialVersionUID = 1L; 

                    // For every key, its values will be passed to the call method in succession 
                    // to aggregate a value corresponding to every key. 
                    // Afterwards, the values corresponding to every key are combined into a Tuple2 as an element of the new RDD. 
                    @Override 
                    public Integer call(Integer v1, Integer v2) throws Exception { 
                        return v1 + v2; 
                    } 

                }); 

        // Print totalScores RDD. 
        totalScores.foreach(new VoidFunction<Tuple2<String,Integer>>() { 

            private static final long serialVersionUID = 1L; 

            @Override 
            public void call(Tuple2<String, Integer> t) throws Exception { 
                System.out.println(t._1 + ": " + t._2);   
            } 

        }); 

        // Disable JavaSparkContext. 
        sc.close(); 
    } 

    /** 
     * The case of sortByKey: Sort by the score of the student. 
     */ 
    private static void sortByKey() { 
        // Create SparkConf. 
        SparkConf conf = new SparkConf() 
                .setAppName("sortByKey")   
                .setMaster("local"); 
        // Create JavaSparkContext. 
        JavaSparkContext sc = new JavaSparkContext(conf); 

        // Simulate a set. 
        List<Tuple2<Integer, String>> scoreList = Arrays.asList( 
                new Tuple2<Integer, String>(65, "leo"), 
                new Tuple2<Integer, String>(50, "tom"), 
                new Tuple2<Integer, String>(100, "marry"), 
                new Tuple2<Integer, String>(80, "jack")); 

        // Parallelize sets and create the RDD. 
        JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList); 

        // Execute the sortByKey operator on scores RDD. 
        // The sortByKey operator is actually sorting by the key. You can manually specify the ascending or descending order. 
        // The returned value is still the JavaPairRDD. The element content in it is identical with that in the original RDD. 
        // But the order of the elements in the RDD is changed. 
        JavaPairRDD<Integer, String> sortedScores = scores.sortByKey(false);   

        // Print sortedScored RDD. 
        sortedScores.foreach(new VoidFunction<Tuple2<Integer,String>>() { 

            private static final long serialVersionUID = 1L; 

            @Override 
            public void call(Tuple2<Integer, String> t) throws Exception { 
                System.out.println(t._1 + ": " + t._2);   
            } 

        }); 

        // Disable JavaSparkContext. 
        sc.close(); 
    } 

    /** 
     * The case of join: Print the scores of the student. 
     */ 
    private static void join() { 
        // Create SparkConf. 
        SparkConf conf = new SparkConf() 
                .setAppName("join")   
                .setMaster("local"); 
        // Create JavaSparkContext. 
        JavaSparkContext sc = new JavaSparkContext(conf); 

        // Simulate a set. 
        List<Tuple2<Integer, String>> studentList = Arrays.asList( 
                new Tuple2<Integer, String>(1, "leo"), 
                new Tuple2<Integer, String>(2, "jack"), 
                new Tuple2<Integer, String>(3, "tom")); 

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList( 
                new Tuple2<Integer, Integer>(1, 100), 
                new Tuple2<Integer, Integer>(2, 90), 
                new Tuple2<Integer, Integer>(3, 60), 
                new Tuple2<Integer, Integer>(2, 80), 
                new Tuple2<Integer, Integer>(2, 70)); 

        // Parallelize two RDDs. 
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList); 
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList); 

        // Associate two RDDs with the join operator. 
        // After the join operation, the join will still be conducted according to the key and a JavaPairRDD is returned. 
        // But the first generic type of JavaPairRDD is consistent with the type of the key of the previous two JavaPairRDDs, because the join is conducted through the key. 
        // The second generic type is of the Tuple2<v1, v2> type. The two generic types of Tuple2 are respectively the value types of the original RDD. 
        // The join operator returns every element of the RDD, that is, a pair joined according to the key. 
        // What does that mean? For example, there is a RDD with (1, 1) (1, 2) (1, 3), 
            // and another RDD with (1, 4) (2, 1) (2, 2). 
            // If it is a cogroup operator, it will be (1,((1,2,3),(4))).     
            // But with the join operator, the actual returned values are (1 (1, 4)) (1, (2, 4)) (1, (3, 4)).     
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = students.join(scores); 

        // Print studnetScores RDD. 
        studentScores.foreach( 

                new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() { 

                    private static final long serialVersionUID = 1L; 

                    @Override 
                    public void call(Tuple2<Integer, Tuple2<String, Integer>> t) 
                            throws Exception { 
                        System.out.println("student id: " + t._1);   
                        System.out.println("student name: " + t._2._1);   
                        System.out.println("student score: " + t._2._2); 
                        System.out.println("===============================");   
                    } 

                }); 

        // Disable JavaSparkContext. 
        sc.close(); 
    } 

    /** 
     * The case of cogroup: Print the scores of the student. 
     */ 
    private static void cogroup() { 
        // Create SparkConf. 
        SparkConf conf = new SparkConf() 
                .setAppName("cogroup")   
                .setMaster("local"); 
        // Create JavaSparkContext. 
        JavaSparkContext sc = new JavaSparkContext(conf); 

        // Simulate a set. 
        List<Tuple2<Integer, String>> studentList = Arrays.asList( 
                new Tuple2<Integer, String>(1, "leo"), 
                new Tuple2<Integer, String>(2, "jack"), 
                new Tuple2<Integer, String>(3, "tom")); 

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList( 
                new Tuple2<Integer, Integer>(1, 100), 
                new Tuple2<Integer, Integer>(2, 90), 
                new Tuple2<Integer, Integer>(3, 60), 
                new Tuple2<Integer, Integer>(1, 70), 
                new Tuple2<Integer, Integer>(2, 80), 
                new Tuple2<Integer, Integer>(3, 50)); 

        // Parallelize two RDDs. 
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList); 
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList); 

        // The cogroup operator is different from the join operator. 
        // It is equivalent to putting all the values of a key join to one Iterable set. 
        // The cogroup operator is not easy to illustrate. I hope you can carefully study its mysteries by compiling the cases we provided here. 
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores = 
                students.cogroup(scores); 

        // Print studnetScores RDD. 
        studentScores.foreach( 

                new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() { 

                    private static final long serialVersionUID = 1L; 

                    @Override 
                    public void call( 
                            Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) 
                            throws Exception { 
                        System.out.println("student id: " + t._1);   
                        System.out.println("student name: " + t._2._1);   
                        System.out.println("student score: " + t._2._2); 
                        System.out.println("===============================");   
                    } 

                }); 

        // Disable JavaSparkContext. 
        sc.close(); 
    } 

} 
