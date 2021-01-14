package org.jeecg.modules.mqtt.mapper;

import java.util.List;
import org.jeecg.modules.mqtt.entity.MbpMqttGatewayTopics;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;

/**
 * @Description: 网关主题
 * @Author: jeecg-boot
 * @Date:   2021-01-12
 * @Version: V1.0
 */
public interface MbpMqttGatewayTopicsMapper extends BaseMapper<MbpMqttGatewayTopics> {

	public boolean deleteByMainId(@Param("mainId") String mainId);
    
	public List<MbpMqttGatewayTopics> selectByMainId(@Param("mainId") String mainId);
}
