package org.jeecg.modules.mqtt.entity;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.math.BigDecimal;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.format.annotation.DateTimeFormat;
import org.jeecgframework.poi.excel.annotation.Excel;
import org.jeecg.common.aspect.annotation.Dict;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * @Description: 实时数据
 * @Author: jeecg-boot
 * @Date:   2021-01-12
 * @Version: V1.0
 */
@Data
@TableName("mbp_mqtt_data")
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ApiModel(value="mbp_mqtt_data对象", description="实时数据")
public class MbpMqttData implements Serializable {
    private static final long serialVersionUID = 1L;

	/**id*/
	@TableId(type = IdType.AUTO)
    @ApiModelProperty(value = "id")
    private Integer id;
	/**变量名称*/
	@Excel(name = "变量名称", width = 15)
    @ApiModelProperty(value = "变量名称")
    private String varName;
	/**变量值*/
	@Excel(name = "变量值", width = 15)
    @ApiModelProperty(value = "变量值")
    private String varValue;
	/**是否警告*/
	@Excel(name = "是否警告", width = 15)
    @ApiModelProperty(value = "是否警告")
    private Integer isWarn;
	/**采集时间*/
	@Excel(name = "采集时间", width = 15, format = "yyyy-MM-dd HH:mm:ss")
	@JsonFormat(timezone = "GMT+8",pattern = "yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern="yyyy-MM-dd HH:mm:ss")
    @ApiModelProperty(value = "采集时间")
    private Date readTime;
	/**创建人*/
    @ApiModelProperty(value = "创建人")
    private String createBy;
	/**创建时间*/
	@JsonFormat(timezone = "GMT+8",pattern = "yyyy-MM-dd")
    @DateTimeFormat(pattern="yyyy-MM-dd")
    @ApiModelProperty(value = "创建时间")
    private Date createTime;
	/**修改人*/
    @ApiModelProperty(value = "修改人")
    private String updateBy;
	/**修改时间*/
	@JsonFormat(timezone = "GMT+8",pattern = "yyyy-MM-dd")
    @DateTimeFormat(pattern="yyyy-MM-dd")
    @ApiModelProperty(value = "修改时间")
    private Date updateTime;
	/**是否删除*/
	@Excel(name = "是否删除", width = 15)
    @ApiModelProperty(value = "是否删除")
    private Integer delFlag;
	/**设备ID*/
	@Excel(name = "设备编号", width = 15)
    @ApiModelProperty(value = "设备ID")
    private String equipmentId;
	/**网关配置*/
	@Excel(name = "网关配置", width = 15)
    @ApiModelProperty(value = "网关配置")
    @Dict(dictTable ="mbp_mqtt_gateway_config",dicText = "gateway_name",dicCode = "id")
    private String configId;
}
