package org.konurbaev.hadoop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RecordSchema {
    private static final Logger logger = LogManager.getLogger(ParquetReaderMapper.class);

    private final List<FieldDescription> fields;

    public RecordSchema(String message) {
        fields = new ArrayList<>();
        List<String> elements = Arrays.asList(message.split("\n"));
        for (String element : elements) {
            String line = element.trim().replace(";", "");
            logger.info("RecordSchema line = " + line);
            if (line.startsWith("optional") || line.startsWith("required")) {
                String[] parts = line.split(" ");
                FieldDescription field = new FieldDescription();
                field.constraint = parts[0];
                field.type = parts[1];
                field.name = parts[2];
                fields.add(field);
            }
        }
    }

    public List<FieldDescription> getFields() {
        return fields;
    }
}
