package org.jeecg.modules.mqtt.util.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.jeecg.modules.mqtt.entity.MbpMqttGatewayConfig;

/**
 * 杭州三米明德科技有限公司
 * 发布端
 *
 * @author zzm
 * date 2020-12-09
 */
public class PublishSample {

    /**
     * 退送离线消息
     *
     * @param mbpMqttConfig 配置信息
     * @param topic         主题 与要离线的客户端同一主题即可，如多个主题，任意一个即可
     */
    public void destroyMqttClient(MbpMqttGatewayConfig mbpMqttConfig, String topic) throws MqttException {

        // 客户端名称
        String clientId = mbpMqttConfig.getMqttClientId();
        // 内存存储
        MemoryPersistence persistence = new MemoryPersistence();

        // 创建客户端
        MqttClient sampleClient = new MqttClient(mbpMqttConfig.getMqttHost(), clientId, persistence);
        // 创建链接参数
        MqttConnectOptions connOpts = new MqttConnectOptions();
        // 在重新启动和重新连接时记住状态
        connOpts.setCleanSession(false);
        // 设置连接的用户名
        connOpts.setUserName(mbpMqttConfig.getMqttUsername());
        connOpts.setPassword(mbpMqttConfig.getMqttPassword().toCharArray());
        // 建立连接
        sampleClient.connect(connOpts);
        // 创建消息 -- 主动推送取消订阅
        MqttMessage message = new MqttMessage("UNSUBSCRIBE".getBytes());
        // 设置消息的服务质量
        message.setQos(1);
        // 发布消息
        sampleClient.publish(topic, message);
        // 断开连接
        sampleClient.disconnect();
        // 关闭客户端
        sampleClient.close();


    }
}
