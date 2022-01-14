// SparkConnector.java
// singleton class that initializes Spark context for servlets to use

package edu.ucr.cs.bdlab.raptor;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

class SparkConnector {

    // single instance
    private static SparkConnector singleInstance = null;

    // Spark context
    public JavaSparkContext sc;

    // private constructor
    private SparkConnector() {

        // initialize Spark context
        // local[*] tells Spark to run with as many worker threads as there are cores on the machine
        System.out.println("----initializing Spark context");
        sc = new JavaSparkContext(new SparkConf().setAppName("Raptor").setMaster("local"));
        System.out.println("----finished initializing Spark context");
    }

    // get or create instance of SparkConnector class
    public static SparkConnector getInstance() {
        if (singleInstance == null) {
            singleInstance = new SparkConnector();
        }
        return singleInstance;
    }

    // JavaSparkConnector getter
    public JavaSparkContext getSC() {
        return this.sc;
    }
}