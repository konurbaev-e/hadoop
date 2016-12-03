package org.konurbaev.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static org.konurbaev.hadoop.Constants.FILE_URI;
import static org.konurbaev.hadoop.Constants.TARGET_FIELD;

public class GroupDirectParquetReader {

    private static final Logger logger = LogManager.getLogger(GroupDirectParquetReader.class);

    public static void main(String[] args) throws URISyntaxException, IOException {
        File file = new File(new URI(FILE_URI));
        Configuration configuration = new Configuration();
        MessageType schema = ParquetFileReader.open(configuration,new Path(file.getAbsolutePath())).getFileMetaData().getSchema();
        GroupWriteSupport.setSchema(schema, configuration);

        OptionalInt position = IntStream.range(0, schema.getFields().size() - 1).filter(idx -> schema.getFields().get(idx).getName().equals(TARGET_FIELD)).findFirst();
        if (!position.isPresent()){
            logger.error("No target field: " + TARGET_FIELD);
            System.exit(1);
        }

        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file.getAbsolutePath())).withConf(configuration).build();

        int counter = 0;
        Group current = reader.read();
        while (current != null) {
            logger.info("Record #" + ++counter);
            logger.info(current.getGroup(position.getAsInt(),0));
        }
    }
}
