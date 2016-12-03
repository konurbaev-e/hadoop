package org.konurbaev.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.example.data.Group;

import java.io.IOException;
import java.util.List;

public class ParquetReaderMapper extends Mapper<LongWritable, Group, NullWritable, Text> {
    private static final Logger logger = LogManager.getLogger(ParquetReaderMapper.class);

    @Override
    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        logger.info("KEYIN = " + key);
        logger.info("VALUEIN = " + value);
        NullWritable outKey = NullWritable.get();

        // Getting schema
        String schema = context.getConfiguration().get(Constants.SCHEMA);
        logger.info("schema = " + schema);
        RecordSchema recordSchema = new RecordSchema(schema);
        List<FieldDescription> expectedFields = recordSchema.getFields();

        // start debug
        logger.info("start debug");
        for (FieldDescription fieldDescription : expectedFields) {
            logger.info("Field description: " + fieldDescription.constraint + "; " + fieldDescription.type + "; " + fieldDescription.name);
        }
        logger.info("end debug");
        // end debug

        // No public accessor to the column values in a Group, so extract them from the string representation
        String line = value.toString();
        String[] fields = line.split("\n");

        StringBuilder csv = new StringBuilder();
        boolean hasContent = false;
        int i = 0;

        // Look for each expected column
        for (FieldDescription expectedField : expectedFields) {
            if (hasContent) {
                csv.append(',');
                logger.info("csv: " + csv.toString());
            }
            String name = expectedField.name;
            if (fields.length > i) {
                String[] parts = fields[i].split(": ");
                // We assume proper order, but there may be fields missing
                if (parts[0].equals(name)) {
                    boolean mustQuote = (parts[1].contains(",") || parts[1].contains("'"));
                    if (mustQuote) {
                        csv.append('"');
                        logger.info("csv: " + csv.toString());
                    }
                    csv.append(parts[1]);
                    logger.info("csv: " + csv.toString());
                    if (mustQuote) {
                        csv.append('"');
                        logger.info("csv: " + csv.toString());
                    }
                    hasContent = true;
                    i++;
                }
            }
        }
        context.write(outKey, new Text(csv.toString()));
    }

}
