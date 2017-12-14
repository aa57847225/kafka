package com.whl.test;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerTest {

    public void productSend() {

        Properties props = new Properties();
        // 用于建立与kafka集群连接的host/port组；集群格式：host:port,host:port
        props.put("bootstrap.servers", "192.168.0.48:9092");
        // producer需要server接收到数据之后发出的确认接收的信号，此项配置就是指procuder需要多少个这样的确认信号。此配置实际上代表了数据备份的可用性。
        // 以下设置为常用选项：（1）acks=0：设置为0表示producer不需要等待任何确认收到的信息。副本将立即加到socket
        // buffer并认为已经发送。没有任何保障可以保证此种情况下server已经成功接收数据，同时重试配置不会发生作用（因为客户端不知道是否失败）回馈的offset会总是设置为-1；（2）acks=1：
        // 这意味着至少要等待leader已经成功将数据写入本地log，但是并没有等待所有follower是否成功写入。这种情况下，如果follower没有成功备份数据，而此时leader又挂掉，则消息会丢失。（3）acks=all：
        // 这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。，
        props.put("acks", "all");
        // 设置大于0的值将使客户端重新发送任何数据，一旦这些数据发送失败。注意，这些重试与客户端接收到发送错误时的重试没有什么不同。允许重试将潜在的改变数据的顺序，
        // 如果这两个消息记录都是发送到同一个partition，则第一个消息失败第二个发送成功，则第二条消息会比第一条消息出现要早。
        // 默认：0
        props.put("retries", 1);
        // producer将试图批处理消息记录，以减少请求次数。这将改善client与server之间的性能。这项配置控制默认的批量处理消息字节数。不会试图处理大于这个字节数的消息字节数。发送到brokers的请求将包含多个批量处理，其中会包含对每个partition的一个请求。较小的批量处理数值比较少用，并且可能降低吞吐量（0则会仅用批量处理）。
        // 较大的批量处理数值将会浪费更多内存空间，这样就需要分配特定批量处理数值的内存大小。
        // 默认值：16384
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        // producer用于压缩数据的压缩类型。默认是无压缩。正确的选项值是none、gzip、snappy。
        // 压缩最好用于批量处理，批量处理消息越多，压缩性能越好; 默认：none
        props.put("compression.type", "none");
        //props.put("advertised.host.name", "192.168.0.48");
        // producer可以用来缓存数据的内存大小。如果数据产生速度大于向broker发送的速度，producer会阻塞或者抛出异常，以“block.on.buffer.full”来表明。
        // 这项设置将和producer能够使用的总内存相关，但并不是一个硬性的限制，因为不是producer使用的所有内存都是用于缓存。一些额外的内存会用于压缩（如果引入压缩机制），同样还有一些用于维护请求。
        // 默认值：33554432
        props.put("buffer.memory", 1116384);
        // key的序列化方式，若是没有设置，同serializer.class
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化类方式
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {

            try {
                Thread.sleep(300);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }

            long runtime = new Date().getTime();
            String topic = "test18";// 主题
            String key = "" + i;// 值
            String msg = "my_msg=============================================" + runtime;
            msg = msg +msg;
            Future<RecordMetadata> result = producer.send(new ProducerRecord<String, String>(topic, key, msg));
            producer.flush();
            try {
                RecordMetadata data = result.get();
                String dmsg = data.topic();
                System.out.println(dmsg + ";partition " + data.partition());

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }

        producer.close();

    }

    public static void main(String[] args) {
        KafkaProducerTest t = new KafkaProducerTest();
        t.productSend();

    }
}
