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
import java.util.*;
import java.util.stream.Collectors;

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
            Map<String, Object> counters = new HashMap<>();
            Optional<SimpleMapRecord> firstLevel = simpleRecord.getValues()
                    .stream()
                    .filter(record -> record.getName().equals(TARGET_FIELD) && record.getValue() instanceof SimpleMapRecord)
                    .map(record -> (SimpleMapRecord) record.getValue())
                    .findFirst();
            if (firstLevel.isPresent()) {
                List<List<SimpleRecord.NameValue>> secondLevel = firstLevel.get().getValues().stream().map(k -> (SimpleRecord) k.getValue()).map(SimpleRecord::getValues).collect(Collectors.toList());

                for (List<SimpleRecord.NameValue> aSecondLevel : secondLevel) {
                    String currentKey = null;
                    for (SimpleRecord.NameValue anASecondLevel : aSecondLevel) {
                        if (anASecondLevel.getName().equals("key")) {
                            currentKey = (String) anASecondLevel.getValue();
                            continue;
                        }
                        counters.put(currentKey, anASecondLevel.getValue());
                    }
                }
                // debug
                counters.forEach((key, value) -> logger.info("key: " + key + "; value: " + value));
                // end debug
            }
        }
        logger.info("ending work ...");
    }
}







