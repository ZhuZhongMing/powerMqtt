package org.jeecg.modules.mqtt.service;

import org.jeecg.modules.mqtt.entity.MbpMqttGatewayTopics;
import com.baomidou.mybatisplus.extension.service.IService;
import java.util.List;

/**
 * @Description: 网关主题
 * @Author: jeecg-boot
 * @Date:   2021-01-12
 * @Version: V1.0
 */
public interface IMbpMqttGatewayTopicsService extends IService<MbpMqttGatewayTopics> {

	public List<MbpMqttGatewayTopics> selectByMainId(String mainId);
}
