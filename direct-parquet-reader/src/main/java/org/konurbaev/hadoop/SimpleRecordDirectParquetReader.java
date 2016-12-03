package org.konurbaev.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.tools.read.SimpleMapRecord;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.konurbaev.hadoop.Constants.FILE_URI;
import static org.konurbaev.hadoop.Constants.TARGET_FIELD;

public class SimpleRecordDirectParquetReader {

    private static final Logger logger = LogManager.getLogger(SimpleRecordDirectParquetReader.class);

    public static void main(String[] args) throws URISyntaxException, IOException {
        File file = new File(new URI(FILE_URI));
        Configuration configuration = new Configuration();

        ParquetReader<SimpleRecord> reader = ParquetReader.builder(new SimpleReadSupport(), new Path(file.getAbsolutePath())).withConf(configuration).build();

        int counter = 0;
        for (SimpleRecord simpleRecord = reader.read(); simpleRecord != null; simpleRecord = reader.read()) {
            logger.info("Record #" + ++counter);
            simpleRecord.getValues()
                    .stream()
                    .filter(record -> record.getName().equals(TARGET_FIELD) && record.getValue() instanceof SimpleMapRecord)
                    .map(record -> (SimpleMapRecord)record.getValue())
                    .findFirst()
                    .ifPresent(simpleMapRecord -> simpleMapRecord.getValues()
                            .forEach(nameValue -> logger.info(nameValue.getName() + ": " + nameValue.getValue())));
        }
    }







}
