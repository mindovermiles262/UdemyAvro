package com.andyduss.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MyNestedGenericRecord {
    public static void main(String[] args) throws IOException {
        //
        // Load Schema
        //

        Schema.Parser parser = new Schema.Parser();
        // File is relative to project base directory
        File schemaFile = new File("src/main/resources/nested_schema.avsc");
        Schema schema = parser.parse(schemaFile);

        //
        // Build a Generic Record
        //

        GenericRecordBuilder grBuilder = new GenericRecordBuilder(schema);
        grBuilder.set("name", "Andy");
        grBuilder.set("age", 30);
        grBuilder.set("favorite_color", "Blue");

        // Build nested record object (manager)
        Schema managerSchema = schema.getField("manager").schema();
        GenericRecordBuilder mgrBuilder = new GenericRecordBuilder(managerSchema);
        mgrBuilder.set("id", 2);
        mgrBuilder.set("code", "base64encoded==");
        GenericData.Record manager = mgrBuilder.build();

        // Attach manager to base Generic Record
        grBuilder.set("manager", manager);

        // Build User
        GenericData.Record user = grBuilder.build();
        System.out.println("Built user: " + user);

        //
        // Write User to File
        //

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

        try {
            Schema userSchema = user.getSchema();
            File outFile = new File("src/main/resources/nested_user.avro");
            dataFileWriter.create(userSchema, outFile);
            dataFileWriter.append(user);
            dataFileWriter.close();
            System.out.println("[+] Wrote user to " + outFile.getPath());
        } catch (Exception e) {
            System.out.println("[!] Error Writing to File");
            e.printStackTrace();
            System.exit(1);
        }

        //
        // Read User from File
        //

        File userLoadFile = new File("src/main/resources/nested_user.avro");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord userRead;
        DataFileReader<GenericRecord> userFileReader = new DataFileReader<>(userLoadFile, datumReader);
        try {
            userRead = userFileReader.next();
            System.out.println("[+] Read from Avro file: " + userLoadFile.getPath());
            System.out.println(userRead);
        } catch(Exception e) {
            System.out.println("[!] Error Reading from Avro File: " + userLoadFile.getPath());
            e.printStackTrace();
        }
    }
}
