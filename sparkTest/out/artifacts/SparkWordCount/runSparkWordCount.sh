#!/bin/bash

/usr/local/spark/bin/spark-submit --class org.yolin.sparkWordCount.wordCount --master spark://127.0.0.1:7077 ./SparkWordCount.jar


