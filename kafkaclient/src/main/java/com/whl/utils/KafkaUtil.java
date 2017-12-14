package com.whl.utils;

import kafka.admin.AdminUtils;
//import kafka.admin.RackAwareMode;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.serializer.StringEncoder;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import kafka.producer.ProducerConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.zookeeper.ZKUtil;
import scala.util.parsing.combinator.testing.Str;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class KafkaUtil {

    /**
     * 创建主题
     * @param serverAddress example:192.168.0.48:2181
     * @param topicName      example:test6
     * @return true/false
     */
    public static boolean createTopic(String serverAddress,String topicName){
        boolean isCreate = false;
        ZkUtils zkUtils = null;
        try{
            zkUtils = ZkUtils.apply(serverAddress, 30000, 30000, JaasUtils.isZkSecurityEnabled());
            // 创建一个单分区单副本名为t1的topic
//            AdminUtils.createTopic(zkUtils, topicName, 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
            zkUtils.close();
            isCreate = true;
        }catch (Exception e){
            //e.printStackTrace();
            System.out.println(e.getMessage());
        }finally {
            if(zkUtils != null){
                zkUtils.close();
            }
        }
        return isCreate;
    }

    /**
     * 查询 topic
     * @param serverAddress
     * @param topicName
     * @return
     */
    public static Iterator searchTopic(String serverAddress,String topicName){
        ZkUtils zkUtils = null;
        Iterator iterator = null;
        try{
            zkUtils = ZkUtils.apply(serverAddress, 30000, 30000, JaasUtils.isZkSecurityEnabled());
            // 获取topic 'test'的topic属性属性
                        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topicName);
            // 查询topic-level属性
            iterator = props.entrySet().iterator();
            return iterator;
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            if(zkUtils != null){
                zkUtils.close();
            }
        }
        return iterator;
    }

    /**
     * topic 是否存在
     * @param serverAddress
     * @param topicName
     * @return
     */
    public static boolean topicExist(String serverAddress, String topicName){
        ZkUtils zkUtils = null;
        boolean isExist = false;
        try{
            zkUtils = ZkUtils.apply(serverAddress, 30000, 30000, JaasUtils.isZkSecurityEnabled());
            // topic 是否存在
            isExist = AdminUtils.topicExists(zkUtils,topicName);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            if(zkUtils != null){
                zkUtils.close();
            }
        }
        return isExist;
    }

    /**
     * 删除topic
     * @param serverAddress
     * @param topicName
     * @return
     */
    public static boolean deleteTopic(String serverAddress,String topicName){
        boolean isDelete = false;
        ZkUtils zkUtils = null;
        try{
            zkUtils = ZkUtils.apply(serverAddress, 30000, 30000, JaasUtils.isZkSecurityEnabled());
            // 删除topic
            AdminUtils.deleteTopic(zkUtils, topicName);
            isDelete = true;
        }catch (Exception e){
            //e.printStackTrace();
            System.out.println(e.getMessage());
        }finally {
            if(zkUtils != null){
                zkUtils.close();
            }
        }
        return isDelete;
    }

    public static void main(String[] args) {
        String serverAddress = "192.168.0.48:2181";
        String topicName = "test5";

//        boolean isCreate =  KafkaUtil.createTopic(serverAddress,topicName);
//        System.out.println(" topic is create ? "+isCreate);

//        boolean isDelete = KafkaUtil.deleteTopic(serverAddress,topicName);
//        System.out.println(isDelete);

//        boolean isExist = KafkaUtil.topicExist(serverAddress,topicName);
//        System.out.println(" topic is exist ? " + isExist);

//        Iterator iterator = KafkaUtil.searchTopic(serverAddress,topicName);
//        while(iterator.hasNext()){
//            Map.Entry entry=(Map.Entry)iterator.next();
//            Object key = entry.getKey();
//            Object value = entry.getValue();
//            System.out.println(key + " = " + value);
//        }
    }
}
