package com.andyduss.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class MyGenericRecord {
    public static void main(String[] args) throws IOException {
        // Load Schema
        Schema.Parser parser = new Schema.Parser();
        // File is relative to project base directory
        File schemaFile = new File("src/main/resources/schema.avsc");
        Schema schema = parser.parse(schemaFile);

        // Build a Generic Record
        GenericRecordBuilder grBuilder = new GenericRecordBuilder(schema);
        grBuilder.set("name", "Klaira");
        grBuilder.set("age", 29);
        grBuilder.set("favorite_color", "Green");

        // Build nested record object (manager)
        Map<String, Object> managerMap =new HashMap<String, Object>();
        int id = 1;
        String code = "base64encoded==";
        managerMap.put("id", id);
        managerMap.put("code", code);


        // Attach manager
        grBuilder.set("manager", managerMap);

        // Build User
        GenericData.Record user = grBuilder.build();

        System.out.println(user);
    }
}
