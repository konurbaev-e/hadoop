package org.konurbaev.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class ParquetReaderJob extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ParquetReaderJob.class);
    public static MessageType schema;

    public static void main(String[] args) throws Exception {
        logger.info("ParquetReaderJob is starting ...");
        logger.info("args[0]" + args[0]);
        logger.info("args[1]" + args[1]);
        try {
            int res = ToolRunner.run(new Configuration(), new ParquetReaderJob(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    public int run(String[] args) throws Exception {
        getConf().set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance();
        job.setJarByClass(ParquetReaderJob.class);
        job.setJobName(getClass().getName());

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        logger.info("Setting Mapper Class ...");
        job.setMapperClass(ParquetReaderMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(ExampleInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //FileStatus fs = FileSystem.get(getConf()).getFileStatus(new Path(args[0]));
        Path parquetFilePath = null;
        RemoteIterator it = FileSystem.get(getConf()).listFiles(new Path(args[0]), true);
        while(it.hasNext()) {
            FileStatus fs = (FileStatus)it.next();
            if(fs.isFile() && (fs.getPath().getName().endsWith(".parquet"))) {
                parquetFilePath = fs.getPath();
                break;
            }
        }
        if(parquetFilePath == null) {
            logger.error("No file found for " + args[0]);
            return 1;
        }
        logger.info("Parquet file = " + parquetFilePath.getName());
        ParquetMetadata readFooter = ParquetFileReader.readFooter(getConf(), parquetFilePath, NO_FILTER);
        schema = readFooter.getFileMetaData().getSchema();
        logger.info("schema = " + schema);

        job.waitForCompletion(true);

        return 0;
    }



}
