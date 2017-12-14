package com.whl.test;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
//import org.apache.commons.codec.DecoderException;
//import org.apache.commons.codec.binary.Hex;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

public class KafkaProducerAvroTest {

    void producer(Schema schema) throws IOException {

        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.0.48:9092");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, byte[]> producer = new Producer<String, byte[]>(config);
        GenericRecord payload1 = new GenericData.Record(schema);
        //Step2 : Put data in that genericrecord object
        payload1.put("desc", "'testdata'");
        //payload1.put("name", "अasa");
        payload1.put("name", "dbevent1");
        payload1.put("id", 111);
        System.out.println("Original Message : "+ payload1);
        //Step3 : Serialize the object to a bytearray
        DatumWriter<GenericRecord>writer = new SpecificDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(payload1, encoder);
        encoder.flush();
        out.close();

        byte[] serializedBytes = out.toByteArray();
        System.out.println("Sending message in bytes : " + serializedBytes);
        KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>("test18", serializedBytes);
        producer.send(message);
        producer.close();

    }


    public static void main(String[] args) throws Exception {
        KafkaProducerAvroTest test = new KafkaProducerAvroTest();

        File file = new File("src/main/resources/test_schema.avsc");
        Schema schema = new Schema.Parser().parse(file);
        test.producer(schema);
    }
}
