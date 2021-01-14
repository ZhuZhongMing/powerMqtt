<template>
  <a-spin :spinning="confirmLoading">
    <j-form-container :disabled="formDisabled">
      <a-form :form="form" slot="detail">
        <a-row>
          <a-col :span="12">
            <a-form-item label="设备ID" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['equipmentId', validatorRules.equipmentId]" placeholder="请输入设备ID"  ></a-input>
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="设备名称" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['equipmentName', validatorRules.equipmentName]" placeholder="请输入设备名称"  ></a-input>
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="设备编号" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['equipmentNum', validatorRules.equipmentNum]" placeholder="请输入设备编号"  ></a-input>
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="设备类型" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <j-category-select v-decorator="['equipmentType', validatorRules.equipmentType]" pcode="B03" placeholder="请选择设备类型"  />
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="所属部门" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <!--<j-select-user-by-dep v-decorator="['deptId', validatorRules.deptId]" />-->
              <!--<j-select-depart v-decorator="['deptId', validatorRules.deptId]" multi  />-->
              <j-select-depart v-decorator="['deptId', validatorRules.deptId]" />
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="说明" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-textarea v-decorator="['disp']" rows="4" placeholder="请输入说明" />
            </a-form-item>
          </a-col>
          <!--<a-col :span="12">
            <a-form-item label="创建人" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['createBy']" placeholder="请输入创建人"  ></a-input>
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="创建时间" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <j-date placeholder="请选择创建时间" v-decorator="['createTime']" :trigger-change="true" style="width: 100%" />
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="修改人" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <a-input v-decorator="['updateBy']" placeholder="请输入修改人"  ></a-input>
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="修改时间" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <j-date placeholder="请选择修改时间" v-decorator="['updateTime']" :trigger-change="true" style="width: 100%" />
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="是否删除" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <j-dict-select-tag type="list" v-decorator="['delFlag']" :trigger-change="true" dictCode="del_flag" placeholder="请选择是否删除" />
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="状态" :labelCol="labelCol" :wrapperCol="wrapperCol">
              <j-dict-select-tag type="list" v-decorator="['equipmentStatus']" :trigger-change="true" dictCode="equipment_status" placeholder="请选择状态" />
            </a-form-item>
          </a-col>-->
          <a-col v-if="showFlowSubmitButton" :span="24" style="text-align: center">
            <a-button @click="submitForm">提 交</a-button>
          </a-col>
        </a-row>
      </a-form>
    </j-form-container>
  </a-spin>
</template>

<script>

  import { httpAction, getAction } from '@/api/manage'
  import pick from 'lodash.pick'
  import { validateDuplicateValue } from '@/utils/util'
  import JFormContainer from '@/components/jeecg/JFormContainer'
  import JDate from '@/components/jeecg/JDate'
  import JSelectUserByDep from '@/components/jeecgbiz/JSelectUserByDep'
  import JSelectDepart from '@/components/jeecgbiz/JSelectDepart'
  import JDictSelectTag from "@/components/dict/JDictSelectTag"
  import JCategorySelect from '@/components/jeecg/JCategorySelect'

  export default {
    name: 'MbpDevEquipmentForm',
    components: {
      JFormContainer,
      JDate,
      JSelectUserByDep,
      JDictSelectTag,
      JCategorySelect,
      JSelectDepart
    },
    props: {
      //流程表单data
      formData: {
        type: Object,
        default: ()=>{},
        required: false
      },
      //表单模式：true流程表单 false普通表单
      formBpm: {
        type: Boolean,
        default: false,
        required: false
      },
      //表单禁用
      disabled: {
        type: Boolean,
        default: false,
        required: false
      }
    },
    data () {
      return {
        form: this.$form.createForm(this),
        model: {},
        labelCol: {
          xs: { span: 24 },
          sm: { span: 5 },
        },
        wrapperCol: {
          xs: { span: 24 },
          sm: { span: 16 },
        },
        confirmLoading: false,
        validatorRules: {
          equipmentId: {
            rules: [
              { required: true, message: '请输入设备ID!'},
              { validator: (rule, value, callback) => validateDuplicateValue('mbp_dev_equipment', 'equipment_id', value, this.model.id, callback)},
            ]
          },
          equipmentName: {
            rules: [
              { required: true, message: '请输入设备名称!'},
              { validator: (rule, value, callback) => validateDuplicateValue('mbp_dev_equipment', 'equipment_name', value, this.model.id, callback)},
            ]
          },
          equipmentNum: {
            rules: [
              { required: true, message: '请输入设备编号!'},
              { validator: (rule, value, callback) => validateDuplicateValue('mbp_dev_equipment', 'equipment_num', value, this.model.id, callback)},
            ]
          },
          equipmentType: {
            rules: [
              { required: true, message: '请输入设备类型!'},
            ]
          },
          deptId: {
            rules: [
              { required: true, message: '请输入所属部门!'},
            ]
          },
        },
        url: {
          add: "/mqtt/mbpDevEquipment/add",
          edit: "/mqtt/mbpDevEquipment/edit",
          queryById: "/mqtt/mbpDevEquipment/queryById"
        }
      }
    },
    computed: {
      formDisabled(){
        if(this.formBpm===true){
          if(this.formData.disabled===false){
            return false
          }
          return true
        }
        return this.disabled
      },
      showFlowSubmitButton(){
        if(this.formBpm===true){
          if(this.formData.disabled===false){
            return true
          }
        }
        return false
      }
    },
    created () {
      //如果是流程中表单，则需要加载流程表单data
      this.showFlowData();
    },
    methods: {
      add () {
        this.edit({});
      },
      edit (record) {
        this.form.resetFields();
        this.model = Object.assign({}, record);
        this.visible = true;
        this.$nextTick(() => {
          this.form.setFieldsValue(pick(this.model, 'equipmentId', 'equipmentName','equipmentNum','equipmentType','deptId','disp','createBy','createTime','updateBy','updateTime','delFlag','equipmentStatus'))
        })
      },
      //渲染流程表单数据
      showFlowData(){
        if(this.formBpm === true){
          let params = {id:this.formData.dataId};
          getAction(this.url.queryById,params).then((res)=>{
            if(res.success){
              this.edit (res.result);
            }
          });
        }
      },
      submitForm () {
        const that = this;
        // 触发表单验证
        this.form.validateFields((err, values) => {
          if (!err) {
            that.confirmLoading = true;
            let httpurl = '';
            let method = '';
            if(!this.model.id){
              httpurl+=this.url.add;
              method = 'post';
            }else{
              httpurl+=this.url.edit;
               method = 'put';
            }
            let formData = Object.assign(this.model, values);
            console.log("表单提交数据",formData)
            httpAction(httpurl,formData,method).then((res)=>{
              if(res.success){
                that.$message.success(res.message);
                that.$emit('ok');
              }else{
                that.$message.warning(res.message);
              }
            }).finally(() => {
              that.confirmLoading = false;
            })
          }

        })
      },
      popupCallback(row){
        this.form.setFieldsValue(pick(row, 'equipmentId', 'equipmentName','equipmentNum','equipmentType','deptId','disp','createBy','createTime','updateBy','updateTime','delFlag','equipmentStatus'))
      },
      handleCategoryChange(value,backObj){
        this.form.setFieldsValue(backObj)
      }
    }
  }
</script>