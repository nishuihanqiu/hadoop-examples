package com.lls.app.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/************************************
 * AvroUtils
 * @author liliangshan
 * @date 2019/11/21
 ************************************/
public class AvroUtils {

    private User user1;
    private User user2;
    private User user3;


    private void createUser() {
        user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);

        user2 = new User("Ben", 7, "red");

        user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteNumber(null)
                .setFavoriteColor("blue")
                .build();
    }

    private void serialize() throws IOException {
        // Serialize user1, user2 and user3 to disk
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("src/main/resources/users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
    }

    private void deserialize() throws IOException {
        DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<>(new File("src/main/resources/users.avro"),
                userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }

    public static void main(String[] args) throws Exception {
        AvroUtils utils = new AvroUtils();
        utils.createUser();
        utils.serialize();
        utils.deserialize();
    }

}
