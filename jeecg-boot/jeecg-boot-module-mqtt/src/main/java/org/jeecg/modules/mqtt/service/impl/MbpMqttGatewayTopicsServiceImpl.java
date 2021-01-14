package org.jeecg.modules.mqtt.service.impl;

import org.jeecg.modules.mqtt.entity.MbpMqttGatewayTopics;
import org.jeecg.modules.mqtt.mapper.MbpMqttGatewayTopicsMapper;
import org.jeecg.modules.mqtt.service.IMbpMqttGatewayTopicsService;
import org.springframework.stereotype.Service;
import java.util.List;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @Description: 网关主题
 * @Author: jeecg-boot
 * @Date:   2021-01-12
 * @Version: V1.0
 */
@Service
public class MbpMqttGatewayTopicsServiceImpl extends ServiceImpl<MbpMqttGatewayTopicsMapper, MbpMqttGatewayTopics> implements IMbpMqttGatewayTopicsService {
	
	@Autowired
	private MbpMqttGatewayTopicsMapper mbpMqttGatewayTopicsMapper;
	
	@Override
	public List<MbpMqttGatewayTopics> selectByMainId(String mainId) {
		return mbpMqttGatewayTopicsMapper.selectByMainId(mainId);
	}
}
