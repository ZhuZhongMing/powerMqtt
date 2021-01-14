package org.jeecg.modules.mqtt.controller;

import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.jeecg.modules.mqtt.util.mqtt.MqttCallBackUtil;
import org.jeecg.modules.mqtt.util.mqtt.PublishSample;
import org.jeecgframework.poi.excel.ExcelImportUtil;
import org.jeecgframework.poi.excel.def.NormalExcelConstants;
import org.jeecgframework.poi.excel.entity.ExportParams;
import org.jeecgframework.poi.excel.entity.ImportParams;
import org.jeecgframework.poi.excel.view.JeecgEntityExcelView;
import org.jeecg.common.system.vo.LoginUser;
import org.apache.shiro.SecurityUtils;
import org.jeecg.common.api.vo.Result;
import org.jeecg.common.system.query.QueryGenerator;
import org.jeecg.common.util.oConvertUtils;
import org.jeecg.modules.mqtt.entity.MbpMqttGatewayTopics;
import org.jeecg.modules.mqtt.entity.MbpMqttGatewayConfig;
import org.jeecg.modules.mqtt.vo.MbpMqttGatewayConfigPage;
import org.jeecg.modules.mqtt.service.IMbpMqttGatewayConfigService;
import org.jeecg.modules.mqtt.service.IMbpMqttGatewayTopicsService;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.extern.slf4j.Slf4j;
import com.alibaba.fastjson.JSON;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.jeecg.common.aspect.annotation.AutoLog;

 /**
 * @Description: 网关配置信息
 * @Author: jeecg-boot
 * @Date:   2021-01-12
 * @Version: V1.0
 */
@Api(tags="网关配置信息")
@RestController
@RequestMapping("/mqtt/mbpMqttGatewayConfig")
@Slf4j
public class MbpMqttGatewayConfigController {
	@Autowired
	private IMbpMqttGatewayConfigService mbpMqttGatewayConfigService;
	@Autowired
	private IMbpMqttGatewayTopicsService mbpMqttGatewayTopicsService;

	 /**
	  * 开启订阅
	  *
	  * @param mbpMqttConfig
	  * @return
	  */
	 @AutoLog(value = "MQTT配置信息-开启订阅")
	 @ApiOperation(value="MQTT配置信息-开启订阅", notes="MQTT配置信息-开启订阅")
	 @GetMapping(value = "/startSub")
	 public Result<?> startSub(MbpMqttGatewayConfig mbpMqttConfig) {
		 MbpMqttGatewayConfig byId = mbpMqttGatewayConfigService.getById(mbpMqttConfig.getId());
		 if (byId.getDelFlag() == 1) {
			 return Result.error("客户端在线！请勿重复操作！");
		 }
		 QueryWrapper<MbpMqttGatewayTopics> queryWapper = new QueryWrapper<>();
		 queryWapper.eq("mqtt_config_id", byId.getId());
		 List<MbpMqttGatewayTopics> list = mbpMqttGatewayTopicsService.list(queryWapper);
		 List<String> topicList = list.stream().map(MbpMqttGatewayTopics::getTopic).collect(Collectors.toList());
		 String[] topics = new String[topicList.size()];
		 Stream.iterate(0, i -> i + 1).limit(topicList.size()).forEach(i -> {
			 topics[i] = topicList.get(i);
		 });
		 byId.setTopics(topics);
		 try {
			 MqttCallBackUtil util = new MqttCallBackUtil();
			 util.run(byId);
		 } catch (Exception e) {
			 log.info("MQTT连接异常：" + e);
			 return Result.error("连接失败");
		 }
		 return Result.OK("连接成功！");
	 }

	 /**
	  * 取消订阅
	  *
	  * @param mbpMqttConfig
	  * @return
	  */
	 @AutoLog(value = "MQTT配置信息-取消订阅")
	 @ApiOperation(value="MQTT配置信息-取消订阅", notes="MQTT配置信息-取消订阅")
	 @GetMapping(value = "/stopSub")
	 public Result<?> stopSub(MbpMqttGatewayConfig mbpMqttConfig) {
		 MbpMqttGatewayConfig byId = mbpMqttGatewayConfigService.getById(mbpMqttConfig.getId());
		 QueryWrapper<MbpMqttGatewayTopics> queryWrapper = new QueryWrapper<>();
		 queryWrapper.eq("mqtt_config_id",byId.getId());
		 List<MbpMqttGatewayTopics> list = mbpMqttGatewayTopicsService.list(queryWrapper);
		 PublishSample publishSample = new PublishSample();
		 try {
			 publishSample.destroyMqttClient(mbpMqttConfig, list.get(0).getTopic());
		 } catch (MqttException e) {
			 log.info("关闭订阅时异常：" + e );
			 return Result.error("操作失败");
		 }
		 MbpMqttGatewayConfig config = new MbpMqttGatewayConfig();
		 config.setId(mbpMqttConfig.getId());
		 config.setDelFlag(0);
		 mbpMqttGatewayConfigService.updateById(config);
		 log.info("客户端【" + byId.getMqttClientId() +"】取消订阅");
		 return Result.OK("操作成功");
	 }
	
	/**
	 * 分页列表查询
	 *
	 * @param mbpMqttGatewayConfig
	 * @param pageNo
	 * @param pageSize
	 * @param req
	 * @return
	 */
	//@AutoLog(value = "网关配置信息-分页列表查询")
	@ApiOperation(value="网关配置信息-分页列表查询", notes="网关配置信息-分页列表查询")
	@GetMapping(value = "/list")
	public Result<?> queryPageList(MbpMqttGatewayConfig mbpMqttGatewayConfig,
								   @RequestParam(name="pageNo", defaultValue="1") Integer pageNo,
								   @RequestParam(name="pageSize", defaultValue="10") Integer pageSize,
								   HttpServletRequest req) {
		QueryWrapper<MbpMqttGatewayConfig> queryWrapper = QueryGenerator.initQueryWrapper(mbpMqttGatewayConfig, req.getParameterMap());
		Page<MbpMqttGatewayConfig> page = new Page<MbpMqttGatewayConfig>(pageNo, pageSize);
		IPage<MbpMqttGatewayConfig> pageList = mbpMqttGatewayConfigService.page(page, queryWrapper);
		return Result.OK(pageList);
	}
	
	/**
	 *   添加
	 *
	 * @param mbpMqttGatewayConfigPage
	 * @return
	 */
	@AutoLog(value = "网关配置信息-添加")
	@ApiOperation(value="网关配置信息-添加", notes="网关配置信息-添加")
	@PostMapping(value = "/add")
	public Result<?> add(@RequestBody MbpMqttGatewayConfigPage mbpMqttGatewayConfigPage) {
		MbpMqttGatewayConfig mbpMqttGatewayConfig = new MbpMqttGatewayConfig();
		BeanUtils.copyProperties(mbpMqttGatewayConfigPage, mbpMqttGatewayConfig);
		mbpMqttGatewayConfigService.saveMain(mbpMqttGatewayConfig, mbpMqttGatewayConfigPage.getMbpMqttGatewayTopicsList());
		return Result.OK("添加成功！");
	}
	
	/**
	 *  编辑
	 *
	 * @param mbpMqttGatewayConfigPage
	 * @return
	 */
	@AutoLog(value = "网关配置信息-编辑")
	@ApiOperation(value="网关配置信息-编辑", notes="网关配置信息-编辑")
	@PutMapping(value = "/edit")
	public Result<?> edit(@RequestBody MbpMqttGatewayConfigPage mbpMqttGatewayConfigPage) {
		log.info("page:" + mbpMqttGatewayConfigPage);
		MbpMqttGatewayConfig mbpMqttGatewayConfig = new MbpMqttGatewayConfig();
		BeanUtils.copyProperties(mbpMqttGatewayConfigPage, mbpMqttGatewayConfig);
		MbpMqttGatewayConfig mbpMqttGatewayConfigEntity = mbpMqttGatewayConfigService.getById(mbpMqttGatewayConfig.getId());
		if(mbpMqttGatewayConfigEntity==null) {
			return Result.error("未找到对应数据");
		}
		mbpMqttGatewayConfigService.updateMain(mbpMqttGatewayConfig, mbpMqttGatewayConfigPage.getMbpMqttGatewayTopicsList());
		return Result.OK("编辑成功!");
	}
	
	/**
	 *   通过id删除
	 *
	 * @param id
	 * @return
	 */
	@AutoLog(value = "网关配置信息-通过id删除")
	@ApiOperation(value="网关配置信息-通过id删除", notes="网关配置信息-通过id删除")
	@DeleteMapping(value = "/delete")
	public Result<?> delete(@RequestParam(name="id",required=true) String id) {
		mbpMqttGatewayConfigService.delMain(id);
		return Result.OK("删除成功!");
	}
	
	/**
	 *  批量删除
	 *
	 * @param ids
	 * @return
	 */
	@AutoLog(value = "网关配置信息-批量删除")
	@ApiOperation(value="网关配置信息-批量删除", notes="网关配置信息-批量删除")
	@DeleteMapping(value = "/deleteBatch")
	public Result<?> deleteBatch(@RequestParam(name="ids",required=true) String ids) {
		this.mbpMqttGatewayConfigService.delBatchMain(Arrays.asList(ids.split(",")));
		return Result.OK("批量删除成功！");
	}
	
	/**
	 * 通过id查询
	 *
	 * @param id
	 * @return
	 */
	@AutoLog(value = "网关配置信息-通过id查询")
	@ApiOperation(value="网关配置信息-通过id查询", notes="网关配置信息-通过id查询")
	@GetMapping(value = "/queryById")
	public Result<?> queryById(@RequestParam(name="id",required=true) String id) {
		MbpMqttGatewayConfig mbpMqttGatewayConfig = mbpMqttGatewayConfigService.getById(id);
		if(mbpMqttGatewayConfig==null) {
			return Result.error("未找到对应数据");
		}
		return Result.OK(mbpMqttGatewayConfig);

	}
	
	/**
	 * 通过id查询
	 *
	 * @param id
	 * @return
	 */
	@AutoLog(value = "网关主题-通过主表ID查询")
	@ApiOperation(value="网关主题-通过主表ID查询", notes="网关主题-通过主表ID查询")
	@GetMapping(value = "/queryMbpMqttGatewayTopicsByMainId")
	public Result<?> queryMbpMqttGatewayTopicsListByMainId(@RequestParam(name="id",required=true) String id) {
		List<MbpMqttGatewayTopics> mbpMqttGatewayTopicsList = mbpMqttGatewayTopicsService.selectByMainId(id);
		IPage <MbpMqttGatewayTopics> page = new Page<>();
		page.setRecords(mbpMqttGatewayTopicsList);
		page.setTotal(mbpMqttGatewayTopicsList.size());
		return Result.OK(page);
	}

    /**
    * 导出excel
    *
    * @param request
    * @param mbpMqttGatewayConfig
    */
    @RequestMapping(value = "/exportXls")
    public ModelAndView exportXls(HttpServletRequest request, MbpMqttGatewayConfig mbpMqttGatewayConfig) {
      // Step.1 组装查询条件查询数据
      QueryWrapper<MbpMqttGatewayConfig> queryWrapper = QueryGenerator.initQueryWrapper(mbpMqttGatewayConfig, request.getParameterMap());
      LoginUser sysUser = (LoginUser) SecurityUtils.getSubject().getPrincipal();

      //Step.2 获取导出数据
      List<MbpMqttGatewayConfig> queryList = mbpMqttGatewayConfigService.list(queryWrapper);
      // 过滤选中数据
      String selections = request.getParameter("selections");
      List<MbpMqttGatewayConfig> mbpMqttGatewayConfigList = new ArrayList<MbpMqttGatewayConfig>();
      if(oConvertUtils.isEmpty(selections)) {
          mbpMqttGatewayConfigList = queryList;
      }else {
          List<String> selectionList = Arrays.asList(selections.split(","));
          mbpMqttGatewayConfigList = queryList.stream().filter(item -> selectionList.contains(item.getId())).collect(Collectors.toList());
      }

      // Step.3 组装pageList
      List<MbpMqttGatewayConfigPage> pageList = new ArrayList<MbpMqttGatewayConfigPage>();
      for (MbpMqttGatewayConfig main : mbpMqttGatewayConfigList) {
          MbpMqttGatewayConfigPage vo = new MbpMqttGatewayConfigPage();
          BeanUtils.copyProperties(main, vo);
          List<MbpMqttGatewayTopics> mbpMqttGatewayTopicsList = mbpMqttGatewayTopicsService.selectByMainId(main.getId());
          vo.setMbpMqttGatewayTopicsList(mbpMqttGatewayTopicsList);
          pageList.add(vo);
      }

      // Step.4 AutoPoi 导出Excel
      ModelAndView mv = new ModelAndView(new JeecgEntityExcelView());
      mv.addObject(NormalExcelConstants.FILE_NAME, "网关配置信息列表");
      mv.addObject(NormalExcelConstants.CLASS, MbpMqttGatewayConfigPage.class);
      mv.addObject(NormalExcelConstants.PARAMS, new ExportParams("网关配置信息数据", "导出人:"+sysUser.getRealname(), "网关配置信息"));
      mv.addObject(NormalExcelConstants.DATA_LIST, pageList);
      return mv;
    }

    /**
    * 通过excel导入数据
    *
    * @param request
    * @param response
    * @return
    */
    @RequestMapping(value = "/importExcel", method = RequestMethod.POST)
    public Result<?> importExcel(HttpServletRequest request, HttpServletResponse response) {
      MultipartHttpServletRequest multipartRequest = (MultipartHttpServletRequest) request;
      Map<String, MultipartFile> fileMap = multipartRequest.getFileMap();
      for (Map.Entry<String, MultipartFile> entity : fileMap.entrySet()) {
          MultipartFile file = entity.getValue();// 获取上传文件对象
          ImportParams params = new ImportParams();
          params.setTitleRows(2);
          params.setHeadRows(1);
          params.setNeedSave(true);
          try {
              List<MbpMqttGatewayConfigPage> list = ExcelImportUtil.importExcel(file.getInputStream(), MbpMqttGatewayConfigPage.class, params);
              for (MbpMqttGatewayConfigPage page : list) {
                  MbpMqttGatewayConfig po = new MbpMqttGatewayConfig();
                  BeanUtils.copyProperties(page, po);
                  mbpMqttGatewayConfigService.saveMain(po, page.getMbpMqttGatewayTopicsList());
              }
              return Result.OK("文件导入成功！数据行数:" + list.size());
          } catch (Exception e) {
              log.error(e.getMessage(),e);
              return Result.error("文件导入失败:"+e.getMessage());
          } finally {
              try {
                  file.getInputStream().close();
              } catch (IOException e) {
                  e.printStackTrace();
              }
          }
      }
      return Result.OK("文件导入失败！");
    }

}
