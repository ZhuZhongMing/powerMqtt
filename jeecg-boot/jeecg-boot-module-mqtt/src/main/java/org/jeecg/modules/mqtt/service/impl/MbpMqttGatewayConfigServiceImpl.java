package org.jeecg.modules.mqtt.service.impl;

import org.jeecg.modules.mqtt.entity.MbpMqttGatewayConfig;
import org.jeecg.modules.mqtt.entity.MbpMqttGatewayTopics;
import org.jeecg.modules.mqtt.mapper.MbpMqttGatewayTopicsMapper;
import org.jeecg.modules.mqtt.mapper.MbpMqttGatewayConfigMapper;
import org.jeecg.modules.mqtt.service.IMbpMqttGatewayConfigService;
import org.springframework.stereotype.Service;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import java.io.Serializable;
import java.util.List;
import java.util.Collection;

/**
 * @Description: 网关配置信息
 * @Author: jeecg-boot
 * @Date:   2021-01-12
 * @Version: V1.0
 */
@Service
public class MbpMqttGatewayConfigServiceImpl extends ServiceImpl<MbpMqttGatewayConfigMapper, MbpMqttGatewayConfig> implements IMbpMqttGatewayConfigService {

	@Autowired
	private MbpMqttGatewayConfigMapper mbpMqttGatewayConfigMapper;
	@Autowired
	private MbpMqttGatewayTopicsMapper mbpMqttGatewayTopicsMapper;
	
	@Override
	@Transactional
	public void saveMain(MbpMqttGatewayConfig mbpMqttGatewayConfig, List<MbpMqttGatewayTopics> mbpMqttGatewayTopicsList) {
		mbpMqttGatewayConfigMapper.insert(mbpMqttGatewayConfig);
		if(mbpMqttGatewayTopicsList!=null && mbpMqttGatewayTopicsList.size()>0) {
			for(MbpMqttGatewayTopics entity:mbpMqttGatewayTopicsList) {
				//外键设置
				entity.setMqttConfigId(mbpMqttGatewayConfig.getId());
				mbpMqttGatewayTopicsMapper.insert(entity);
			}
		}
	}

	@Override
	@Transactional
	public void updateMain(MbpMqttGatewayConfig mbpMqttGatewayConfig,List<MbpMqttGatewayTopics> mbpMqttGatewayTopicsList) {
		mbpMqttGatewayConfigMapper.updateById(mbpMqttGatewayConfig);
		
		//1.先删除子表数据
		mbpMqttGatewayTopicsMapper.deleteByMainId(mbpMqttGatewayConfig.getId());
		
		//2.子表数据重新插入
		if(mbpMqttGatewayTopicsList!=null && mbpMqttGatewayTopicsList.size()>0) {
			for(MbpMqttGatewayTopics entity:mbpMqttGatewayTopicsList) {
				//外键设置
				entity.setMqttConfigId(mbpMqttGatewayConfig.getId());
				mbpMqttGatewayTopicsMapper.insert(entity);
			}
		}
	}

	@Override
	@Transactional
	public void delMain(String id) {
		mbpMqttGatewayTopicsMapper.deleteByMainId(id);
		mbpMqttGatewayConfigMapper.deleteById(id);
	}

	@Override
	@Transactional
	public void delBatchMain(Collection<? extends Serializable> idList) {
		for(Serializable id:idList) {
			mbpMqttGatewayTopicsMapper.deleteByMainId(id.toString());
			mbpMqttGatewayConfigMapper.deleteById(id);
		}
	}
	
}
