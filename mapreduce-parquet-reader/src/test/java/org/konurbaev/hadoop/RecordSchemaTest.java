package org.konurbaev.hadoop;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RecordSchemaTest {

    private static final String schema = "message spark_schema {\n" +
            "  optional binary age (UTF8);\n" +
            "  optional binary id (UTF8);\n" +
            "  optional binary name (UTF8);\n" +
            "}\n";

    @Test
    public void testSchema () {
        RecordSchema recordSchema = new RecordSchema(schema);
        List<FieldDescription> fieldDescriptionList = recordSchema.getFields();
        Assert.assertEquals(fieldDescriptionList.get(0).constraint,"optional");
        Assert.assertEquals(fieldDescriptionList.get(1).constraint,"optional");
        Assert.assertEquals(fieldDescriptionList.get(2).constraint,"optional");
        Assert.assertEquals(fieldDescriptionList.get(0).type,"binary");
        Assert.assertEquals(fieldDescriptionList.get(1).type,"binary");
        Assert.assertEquals(fieldDescriptionList.get(2).type,"binary");
        Assert.assertEquals(fieldDescriptionList.get(0).name,"age");
        Assert.assertEquals(fieldDescriptionList.get(1).name,"id");
        Assert.assertEquals(fieldDescriptionList.get(2).name,"name");
    }
}
