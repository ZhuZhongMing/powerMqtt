package org.jeecg.modules.mqtt.util.mqtt;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.jeecg.modules.mqtt.entity.MbpMqttGatewayConfig;
import org.jeecg.modules.mqtt.service.IMbpMqttGatewayConfigService;

import java.util.Arrays;

/**
 * 杭州三米明德科技有限公司
 * 连接工具类
 * @author zzm
 * date 2020-09-04
 */
@Slf4j
public class MQTTConnentionUtil {

    /**
     * MQTT连接
     * host MQTT 服务器ip地址：1833
     * clientId 客户端id
     * name 用户名
     * password 密码
     * topic 主题S
     * @return MQTT连接
     */
    public static MqttClient getMQTTConnect(MbpMqttGatewayConfig mqtt) throws MqttException { // String host, String clientId, String name, String password, String[] topic
        log.info("host:" + mqtt.getMqttHost());
        log.info("clientId:" + mqtt.getMqttClientId());
        log.info("name:" + mqtt.getMqttUsername());
        log.info("password:" + mqtt.getMqttPassword());
        log.info("topic:" + Arrays.toString(mqtt.getTopics()));
        MemoryPersistence persistence = new MemoryPersistence();
        MqttClient sampleClient = null;
       /* try{*/
            sampleClient = new MqttClient(mqtt.getMqttHost(), mqtt.getMqttClientId(), persistence);
            // 连接设置
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            // 设置连接用户名
            connOpts.setUserName(mqtt.getMqttUsername());
            // 设置连接的密码
            connOpts.setPassword( mqtt.getMqttPassword().toCharArray());
            // 设置超时时间，单位S
            connOpts.setConnectionTimeout(10);
            // 设置会话心跳时间，单位S
            connOpts.setKeepAliveInterval(3600);
            connOpts.setAutomaticReconnect(true);
            sampleClient.connect(connOpts);
            log.info("=============================================》 【MQTT】【" + mqtt.getMqttClientId() + "】连接成功！");
            log.info("=============================================》 服务端地址:" + mqtt.getMqttHost());
            log.info("=============================================》 主题:" + Arrays.toString(mqtt.getTopics()));
            //Integer[] qos = {1,2,3};
            sampleClient.subscribe(mqtt.getTopics());
        /*}catch(MqttException me){
            log.error("【MQTT】【" +  mqtt.getMqttClientId() + "】连接时发生异常！异常信息：" + me);
        }*/
        return sampleClient;
    }


    /**
     *
     * @param sampleClient 客户端连接
     * @param mqtt 客户端mqtt配置信息
     * @param sub
     */
    public static void reconnectionMQTT(MqttClient sampleClient, MbpMqttGatewayConfig mqtt, MqttCallBackUtil sub, IMbpMqttGatewayConfigService mbpMqttConfigService) {
        log.warn("【MQTT】【" + mqtt.getMqttClientId() + "】连接断开，30S后重新尝试重连......");
        MbpMqttGatewayConfig newMqtt = new MbpMqttGatewayConfig();
        newMqtt.setId(mqtt.getId());
        newMqtt.setDelFlag(0);
        while (true) {
            try {
                sampleClient.close();
                Thread.sleep(30000);
                sub.run(mqtt);
                log.info("=========================================================》【MQTT】【" + mqtt.getMqttClientId() + "】重新连接成功");
                break;
            } catch (InterruptedException e) {
                mbpMqttConfigService.updateById(newMqtt);
                Thread.interrupted(); // 重置线程中断状态
                log.error("【MQTT】【" + mqtt.getMqttClientId() + "】重连时发生线程中断异常！异常信息：" + e);
            }catch (Exception e) {
                mbpMqttConfigService.updateById(newMqtt);
                //e.printStackTrace();
                log.error("【MQTT】【" + mqtt.getMqttClientId() + "】重连时发生异常！异常信息：" + e);
            }
        }
    }
}
