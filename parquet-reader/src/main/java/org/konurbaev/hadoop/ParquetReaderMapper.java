package org.konurbaev.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputSplit;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ParquetReaderMapper  extends Mapper<LongWritable, Group, NullWritable, Text> {
    private static final Logger logger = LogManager.getLogger(ParquetReaderMapper.class);
    private static List<FieldDescription> expectedFields = null;

    @Override
    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        logger.info("KEYIN = " + key);
        logger.info("VALUEIN = " + value);
        NullWritable outKey = NullWritable.get();
        if(expectedFields == null) {
            // Get the file schema which may be different from the fields in a particular record) from the input split
            String fileSchema = ((ParquetInputSplit)context.getInputSplit()).getFileSchema();
            logger.info("key = " + key + "; fileSchema" + fileSchema);
            // System.err.println("file schema from context: " + fileSchema);
            RecordSchema schema = new RecordSchema(fileSchema);
            expectedFields = schema.getFields();
            //System.err.println("inferred schema: " + expectedFields.toString());
        }

        // No public accessor to the column values in a Group, so extract them from the string representation
        String line = value.toString();
        String[] fields = line.split("\n");

        StringBuilder csv = new StringBuilder();
        boolean hasContent = false;
        int i = 0;
        // Look for each expected column
        Iterator<FieldDescription> it = expectedFields.iterator();
        while(it.hasNext()) {
            if(hasContent ) {
                csv.append(',');
            }
            String name = it.next().name;
            if(fields.length > i) {
                String[] parts = fields[i].split(": ");
                // We assume proper order, but there may be fields missing
                if(parts[0].equals(name)) {
                    boolean mustQuote = (parts[1].contains(",") || parts[1].contains("'"));
                    if(mustQuote) {
                        csv.append('"');
                    }
                    csv.append(parts[1]);
                    if(mustQuote) {
                        csv.append('"');
                    }
                    hasContent = true;
                    i++;
                }
            }
        }
        context.write(outKey, new Text(csv.toString()));
    }

}
