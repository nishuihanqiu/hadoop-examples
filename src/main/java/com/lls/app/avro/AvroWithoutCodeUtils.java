package com.lls.app.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

/************************************
 * AvroWithoutCodeUtils
 * @author liliangshan
 * @date 2019/11/21
 ************************************/
public class AvroWithoutCodeUtils {

    private void serialize() throws IOException {
        String avscFilePath = "src/main/avro/user.avsc";
        Schema schema = new Schema.Parser().parse(new File(avscFilePath));

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Tony");
        user1.put("favoriteNumber", 18);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favoriteNumber", 3);
        user2.put("favoriteColor", "red");

        File file = new File("src/main/resources/users.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
    }

    private void deserialize() throws IOException {
        String avscFilePath = "src/main/avro/user.avsc";
        Schema schema = new Schema.Parser().parse(new File(avscFilePath));
        File file = new File("src/main/resources/users.avro");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }

    public static void main(String[] args) throws Exception {
        AvroWithoutCodeUtils utils = new AvroWithoutCodeUtils();
        utils.serialize();
        utils.deserialize();
    }

}
