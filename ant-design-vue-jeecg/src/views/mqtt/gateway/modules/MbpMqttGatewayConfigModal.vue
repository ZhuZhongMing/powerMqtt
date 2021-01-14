<template>
  <j-modal
    :title="title"
    :width="1200"
    :visible="visible"
    :maskClosable="false"
    :confirmLoading="confirmLoading"
    switchFullscreen
    @ok="handleOk"
    @cancel="handleCancel">
    <a-spin :spinning="confirmLoading">
      <!-- 主表单区域 -->
      <a-form :form="form">
        <a-row>
          <a-col :xs="24" :sm="12">
            <a-form-item label="网关名称" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['gatewayName', validatorRules.gatewayName]" placeholder="请输入网关名称" ></a-input>
            </a-form-item>
          </a-col>
          <a-col :xs="24" :sm="12">
            <a-form-item label="部门编号" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <j-select-depart v-decorator="['deptId', validatorRules.deptId]" />
            </a-form-item>
          </a-col>
          <a-col :xs="24" :sm="12">
            <a-form-item label="客户端ID" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['mqttClientId', validatorRules.mqttClientId]" placeholder="请输入客户端ID" ></a-input>
            </a-form-item>
          </a-col>
          <a-col :xs="24" :sm="12">
            <a-form-item label="host" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['mqttHost', validatorRules.mqttHost]" placeholder="请输入host" ></a-input>
            </a-form-item>
          </a-col>
          <a-col :xs="24" :sm="12">
            <a-form-item label="用户名" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['mqttUsername', validatorRules.mqttUsername]" placeholder="请输入用户名" ></a-input>
            </a-form-item>
          </a-col>
          <a-col :xs="24" :sm="12">
            <a-form-item label="密码" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['mqttPassword', validatorRules.mqttPassword]" placeholder="请输入密码" ></a-input>
            </a-form-item>
          </a-col>
          <a-col :span="24">
            <a-form-item label="备注" :labelCol="labelCol2" :wrapperCol="wrapperCol2">
              <a-textarea v-decorator="['disp']" rows="4" placeholder="请输入备注" />
            </a-form-item>
          </a-col>
          <!--<a-col :xs="24" :sm="12">
            <a-form-item label="创建人" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['createBy']" placeholder="请输入创建人" ></a-input>
            </a-form-item>
          </a-col>
          <a-col :xs="24" :sm="12">
            <a-form-item label="创建时间" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <j-date placeholder="请选择创建时间" v-decorator="['createTime']" :trigger-change="true" style="width: 100%" />
            </a-form-item>
          </a-col>
          <a-col :xs="24" :sm="12">
            <a-form-item label="修改人" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['updateBy']" placeholder="请输入修改人" ></a-input>
            </a-form-item>
          </a-col>
          <a-col :xs="24" :sm="12">
            <a-form-item label="修改时间" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <j-date placeholder="请选择修改时间" v-decorator="['updateTime']" :trigger-change="true" style="width: 100%" />
            </a-form-item>
          </a-col>
          <a-col :xs="24" :sm="12">
            <a-form-item label="是否删除" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <j-dict-select-tag type="list" v-decorator="['delFlag']" :trigger-change="true" dictCode="del_flag" placeholder="请选择是否删除" />
            </a-form-item>
          </a-col>-->

        </a-row>
      </a-form>

      <!-- 子表单区域 -->
      <a-tabs v-model="activeKey" @change="handleChangeTabs">
        <a-tab-pane tab="网关主题" :key="refKeys[0]" :forceRender="true">
          <j-editable-table
            :ref="refKeys[0]"
            :loading="mbpMqttGatewayTopicsTable.loading"
            :columns="mbpMqttGatewayTopicsTable.columns"
            :dataSource="mbpMqttGatewayTopicsTable.dataSource"
            :maxHeight="300"
            :rowNumber="true"
            :rowSelection="true"
            :actionButton="true"/>
        </a-tab-pane>

      </a-tabs>

    </a-spin>
  </j-modal>
</template>

<script>

  import pick from 'lodash.pick'
  import { FormTypes,getRefPromise } from '@/utils/JEditableTableUtil'
  import { JEditableTableMixin } from '@/mixins/JEditableTableMixin'
  import { validateDuplicateValue } from '@/utils/util'
  import JDate from '@/components/jeecg/JDate'
  import JSelectDepart from '@/components/jeecgbiz/JSelectDepart'
  import JDictSelectTag from "@/components/dict/JDictSelectTag"

  export default {
    name: 'MbpMqttGatewayConfigModal',
    mixins: [JEditableTableMixin],
    components: {
      JDate,
      JSelectDepart,
      JDictSelectTag,
    },
    data() {
      return {
        labelCol: {
          xs: { span: 24 },
          sm: { span: 6 },
        },
        wrapperCol: {
          xs: { span: 24 },
          sm: { span: 16 },
        },
        labelCol2: {
          xs: { span: 24 },
          sm: { span: 3 },
        },
        wrapperCol2: {
          xs: { span: 24 },
          sm: { span: 20 },
        },
        // 新增时子表默认添加几行空数据
        addDefaultRowNum: 1,
        validatorRules: {
          gatewayName: {
            rules: [
              { required: true, message: '请输入网关名称!'},
            ]
          },
          deptId: {
            rules: [
              { required: true, message: '请输入部门编号!'},
            ]
          },
          mqttHost: {
            rules: [
              { required: true, message: '请输入host!'},
            ]
          },
          mqttUsername: {
            rules: [
              { required: true, message: '请输入用户名!'},
            ]
          },
          mqttPassword: {
            rules: [
              { required: true, message: '请输入密码!'},
            ]
          },
          mqttClientId: {
            rules: [
              { required: true, message: '请输入客户端ID!'},
              { validator: (rule, value, callback) => validateDuplicateValue('mbp_mqtt_gateway_config', 'mqtt_client_id', value, this.model.id, callback)},
            ]
          },
        },
        refKeys: ['mbpMqttGatewayTopics', ],
        tableKeys:['mbpMqttGatewayTopics', ],
        activeKey: 'mbpMqttGatewayTopics',
        // 网关主题
        mbpMqttGatewayTopicsTable: {
          loading: false,
          dataSource: [],
          columns: [
            {
              title: '主题',
              key: 'topic',
              type: FormTypes.input,
              width:"200px",
              placeholder: '请输入${title}',
              defaultValue:'',
              validateRules: [{ required: true, message: '${title}不能为空' }],
            },
            {
              title: '说明',
              key: 'disp',
              type: FormTypes.input,
              width:"200px",
              placeholder: '请输入${title}',
              defaultValue:'',
            },
            /*{
              title: '创建人',
              key: 'createBy',
              type: FormTypes.input,
              width:"200px",
              placeholder: '请输入${title}',
              defaultValue:'',
            },
            {
              title: '创建时间',
              key: 'createTime',
              type: FormTypes.date,
              width:"200px",
              placeholder: '请输入${title}',
              defaultValue:'',
            },
            {
              title: '修改人',
              key: 'updateBy',
              type: FormTypes.input,
              width:"200px",
              placeholder: '请输入${title}',
              defaultValue:'',
            },
            {
              title: '修改时间',
              key: 'updateTime',
              type: FormTypes.date,
              width:"200px",
              placeholder: '请输入${title}',
              defaultValue:'',
            },
            {
              title: '是否删除',
              key: 'delFlag',
              type: FormTypes.input,
              width:"200px",
              placeholder: '请输入${title}',
              defaultValue:'',
            },*/
          ]
        },
        url: {
          add: "/mqtt/mbpMqttGatewayConfig/add",
          edit: "/mqtt/mbpMqttGatewayConfig/edit",
          mbpMqttGatewayTopics: {
            list: '/mqtt/mbpMqttGatewayConfig/queryMbpMqttGatewayTopicsByMainId'
          },
        }
      }
    },
    methods: {
      getAllTable() {
        let values = this.tableKeys.map(key => getRefPromise(this, key))
        return Promise.all(values)
      },
      /** 调用完edit()方法之后会自动调用此方法 */
      editAfter() {
        let fieldval = pick(this.model,'gatewayName','deptId','mqttHost','mqttUsername','mqttPassword','mqttClientId','disp','createBy','createTime','updateBy','updateTime','delFlag')
        this.$nextTick(() => {
          this.form.setFieldsValue(fieldval)
        })
        // 加载子表数据
        if (this.model.id) {
          let params = { id: this.model.id }
          this.requestSubTableData(this.url.mbpMqttGatewayTopics.list, params, this.mbpMqttGatewayTopicsTable)
        }
      },
      /** 整理成formData */
      classifyIntoFormData(allValues) {
        let main = Object.assign(this.model, allValues.formValue)

        return {
          ...main, // 展开
          mbpMqttGatewayTopicsList: allValues.tablesValue[0].values,
        }
      },
      validateError(msg){
        this.$message.error(msg)
      },
     popupCallback(row){
       this.form.setFieldsValue(pick(row,'gatewayName','deptId','mqttHost','mqttUsername','mqttPassword','mqttClientId','disp','createBy','createTime','updateBy','updateTime','delFlag'))
     },

    }
  }
</script>

<style scoped>
</style>