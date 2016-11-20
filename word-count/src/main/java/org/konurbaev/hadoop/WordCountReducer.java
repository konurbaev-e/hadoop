package org.konurbaev.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class WordCountReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

    private static final Logger logger = LogManager.getLogger(WordCountMapper.class);

    private final IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("KEYIN = " + key);

        int sum = 0;
        for (IntWritable val : values) {
            logger.info("key = " + key + "; value = " + val.get());
            sum += val.get();
        }
        logger.info("key = " + key + "; sum = " + sum);
        result.set(sum);
        context.write(key, result);
    }
}