package org.jeecg.modules.mqtt.service;

import org.jeecg.modules.mqtt.entity.MbpMqttGatewayTopics;
import org.jeecg.modules.mqtt.entity.MbpMqttGatewayConfig;
import com.baomidou.mybatisplus.extension.service.IService;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * @Description: 网关配置信息
 * @Author: jeecg-boot
 * @Date:   2021-01-12
 * @Version: V1.0
 */
public interface IMbpMqttGatewayConfigService extends IService<MbpMqttGatewayConfig> {

	/**
	 * 添加一对多
	 * 
	 */
	public void saveMain(MbpMqttGatewayConfig mbpMqttGatewayConfig, List<MbpMqttGatewayTopics> mbpMqttGatewayTopicsList) ;
	
	/**
	 * 修改一对多
	 * 
	 */
	public void updateMain(MbpMqttGatewayConfig mbpMqttGatewayConfig, List<MbpMqttGatewayTopics> mbpMqttGatewayTopicsList);
	
	/**
	 * 删除一对多
	 */
	public void delMain(String id);
	
	/**
	 * 批量删除一对多
	 */
	public void delBatchMain(Collection<? extends Serializable> idList);
	
}
