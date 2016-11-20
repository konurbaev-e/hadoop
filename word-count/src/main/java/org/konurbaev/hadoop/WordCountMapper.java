package org.konurbaev.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper <Object, Text, Text, IntWritable> {
    private static final Logger logger = LogManager.getLogger(WordCountMapper.class);

    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        logger.info("KEYIN = " + key);
        logger.info("VALUEIN = " + value);
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line,",");
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
}
