package com.pbn.pnm;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;



public class OnlineWordCount {

    public static void main(String[] args) throws Exception {

        System.out.println("启动成功============");

        //if (args.length < 2) {
            //System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
           // System.exit(1);
       // }


        SparkConf sc = new SparkConf().setAppName("online count");

        JavaStreamingContext jsc = new JavaStreamingContext(sc,Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream(" 192.168.85.45", 9999);


        JavaDStream<String> words = lines.flatMap(strs -> Arrays.asList(strs.split(" ")).iterator());

        JavaPairDStream<String,Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s,1)).reduceByKey((i1,i2) -> i1+i2);

        System.out.println("=====1111111111111111111111111=======");
        wordCounts.print();
        System.out.println("====22222222222222222222222222222222========");

        jsc.start();

        jsc.awaitTermination();

        jsc.stop();

    }
}
