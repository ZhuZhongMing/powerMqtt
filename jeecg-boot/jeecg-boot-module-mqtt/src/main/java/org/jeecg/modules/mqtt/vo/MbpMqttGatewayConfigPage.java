package org.jeecg.modules.mqtt.vo;

import java.util.List;
import org.jeecg.modules.mqtt.entity.MbpMqttGatewayConfig;
import org.jeecg.modules.mqtt.entity.MbpMqttGatewayTopics;
import lombok.Data;
import org.jeecgframework.poi.excel.annotation.Excel;
import org.jeecgframework.poi.excel.annotation.ExcelEntity;
import org.jeecgframework.poi.excel.annotation.ExcelCollection;
import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.format.annotation.DateTimeFormat;
import java.util.Date;
import org.jeecg.common.aspect.annotation.Dict;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * @Description: 网关配置信息
 * @Author: jeecg-boot
 * @Date:   2021-01-12
 * @Version: V1.0
 */
@Data
@ApiModel(value="mbp_mqtt_gateway_configPage对象", description="网关配置信息")
public class MbpMqttGatewayConfigPage {

    /**id*/
    @ApiModelProperty(value = "id")
    private String id;
    /**网关名称*/
    @Excel(name = "网关名称", width = 15)
    @ApiModelProperty(value = "网关名称")
    private String gatewayName;
    /**部门编号*/
    @Excel(name = "部门编号", width = 15, dictTable = "sys_depart", dicText = "depart_name", dicCode = "id")
    @Dict(dictTable = "sys_depart", dicText = "depart_name", dicCode = "id")
    @ApiModelProperty(value = "部门编号")
    private String deptId;
    /**host*/
    @Excel(name = "host", width = 15)
    @ApiModelProperty(value = "host")
    private String mqttHost;
    /**用户名*/
    @Excel(name = "用户名", width = 15)
    @ApiModelProperty(value = "用户名")
    private String mqttUsername;
    /**密码*/
    @Excel(name = "密码", width = 15)
    @ApiModelProperty(value = "密码")
    private String mqttPassword;
    /**客户端ID*/
    @Excel(name = "客户端ID", width = 15)
    @ApiModelProperty(value = "客户端ID")
    private String mqttClientId;
    /**备注*/
    @Excel(name = "备注", width = 15)
    @ApiModelProperty(value = "备注")
    private String disp;
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
    @Excel(name = "是否删除", width = 15, dicCode = "del_flag")
    @Dict(dicCode = "del_flag")
    @ApiModelProperty(value = "是否删除")
    private Integer delFlag;

    @ExcelCollection(name="网关主题")
    @ApiModelProperty(value = "网关主题")
    private List<MbpMqttGatewayTopics> mbpMqttGatewayTopicsList;

}
