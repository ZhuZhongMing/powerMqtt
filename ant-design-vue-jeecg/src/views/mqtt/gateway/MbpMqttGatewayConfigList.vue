<template>
  <a-card class="j-inner-table-wrapper" :bordered="false">

    <!-- 查询区域 begin -->
    <div class="table-page-search-wrapper">
      <a-form layout="inline">
        <a-row :gutter="24">
        </a-row>
      </a-form>
    </div>
    <!-- 查询区域 end -->

    <!-- 操作按钮区域 begin -->
    <div class="table-operator">
      <a-button type="primary" icon="plus" @click="handleAdd">新增</a-button>
      <a-button type="primary" icon="download" @click="handleExportXls('网关配置信息')">导出</a-button>
      <a-upload name="file" :showUploadList="false" :multiple="false" :headers="tokenHeader" :action="importExcelUrl" @change="handleImportExcel">
        <a-button type="primary" icon="import">导入</a-button>
      </a-upload>
      <!-- 高级查询区域 -->
      <j-super-query :fieldList="superFieldList" ref="superQueryModal" @handleSuperQuery="handleSuperQuery"></j-super-query>
      <a-dropdown v-if="selectedRowKeys.length > 0">
        <a-menu slot="overlay">
          <a-menu-item key="1" @click="batchDel">
            <a-icon type="delete"/>
            <span>删除</span>
          </a-menu-item>
        </a-menu>
        <a-button>
          <span>批量操作</span>
          <a-icon type="down"/>
        </a-button>
      </a-dropdown>
    </div>
    <!-- 操作按钮区域 end -->

    <!-- table区域 begin -->
    <div>

      <a-alert type="info" showIcon style="margin-bottom: 16px;">
        <template slot="message">
          <span>已选择</span>
          <a style="font-weight: 600;padding: 0 4px;">{{ selectedRowKeys.length }}</a>
          <span>项</span>
          <a style="margin-left: 24px" @click="onClearSelected">清空</a>
        </template>
      </a-alert>

      <a-table
        ref="table"
        size="middle"
        bordered
        rowKey="id"
        class="j-table-force-nowrap"
        :scroll="{x:true}"
        :loading="loading"
        :columns="columns"
        :dataSource="dataSource"
        :pagination="ipagination"
        :expandedRowKeys="expandedRowKeys"
        :rowSelection="{selectedRowKeys, onChange: onSelectChange}"
        @expand="handleExpand"
        @change="handleTableChange"
      >

        <!-- 内嵌table区域 begin -->
        <template slot="expandedRowRender" slot-scope="record">
          <a-tabs tabPosition="top">
            <a-tab-pane tab="网关主题" key="mbpMqttGatewayTopics" forceRender>
              <mbp-mqtt-gateway-topics-sub-table :record="record"/>
            </a-tab-pane>
          </a-tabs>
        </template>
        <!-- 内嵌table区域 end -->

        <template slot="htmlSlot" slot-scope="text">
          <div v-html="text"></div>
        </template>

        <template slot="imgSlot" slot-scope="text">
          <div style="font-size: 12px;font-style: italic;">
            <span v-if="!text">无图片</span>
            <img v-else :src="getImgView(text)" alt="" style="max-width:80px;height:25px;"/>
          </div>
        </template>

        <template slot="fileSlot" slot-scope="text">
          <span v-if="!text" style="font-size: 12px;font-style: italic;">无文件</span>
          <a-button
            v-else
            ghost
            type="primary"
            icon="download"
            size="small"
            @click="downloadFile(text)"
          >
            <span>下载</span>
          </a-button>
        </template>

        <template slot="action" slot-scope="text, record">
          <a @click="handleEdit(record)">编辑</a>
          <a-divider type="vertical"/>
          <a-dropdown>
            <a class="ant-dropdown-link">
              <span>更多 <a-icon type="down"/></span>
            </a>
            <a-menu slot="overlay">
              <a-menu-item v-if="record.delFlag == 0">
                <a-popconfirm title="确定开始吗?" @confirm="handleStart(record)">
                  <a>开启订阅</a>
                </a-popconfirm>
              </a-menu-item>
              <a-menu-item v-if="record.delFlag == 1">
                <a-popconfirm title="确定查看吗?" @confirm="checkData(record)">
                  <a>实时数据</a>
                </a-popconfirm>
              </a-menu-item>
              <a-menu-item v-if="record.delFlag == 1">
                <a-popconfirm title="确定取消吗?" @confirm="handleStop(record)">
                  <a>取消订阅</a>
                </a-popconfirm>
              </a-menu-item>
              <a-menu-item>
                <a-popconfirm title="确定删除吗?" @confirm="handleDelete(record.id)">
                  <a>删除</a>
                </a-popconfirm>
              </a-menu-item>
            </a-menu>
            <!--<a-menu slot="overlay">
              <a-menu-item v-if="record.delFlag == 0">
                <a-popconfirm title="确定开始吗?" @confirm="handleStart(record)">
                  <a>开启订阅</a>
                </a-popconfirm>
              </a-menu-item>
              <a-menu-item v-if="record.delFlag == 1">
                <a-popconfirm title="确定取消吗?" @confirm="handleStop(record)">
                  <a>取消订阅</a>
                </a-popconfirm>
              </a-menu-item>
              <a-menu-item>
                <a-popconfirm title="确定删除吗?" @confirm="handleDelete(record.id)">
                  <a>删除</a>
                </a-popconfirm>
              </a-menu-item>
            </a-menu>-->
          </a-dropdown>

        </template>



      </a-table>
    </div>
    <!-- table区域 end -->

    <!--实时数据弹窗-->
    <a-modal
      v-model="dataModelVisible"
      :footer="null"
      :maskClosable="false"
      :width="800"
      title="实时数据" onclose="handleModelClose"
      :bodyStyle="{height: '600px', overflowY: 'auto'}">
      <div>
        <a-empty v-if="mqttMssage.length <= 0"/>
        <pre v-else v-html="mqttMssage" id="pre"></pre>
        <!--<p>Some contents...</p>
        <p>Some contents...</p>
        <p>Some contents...</p>-->
      </div>
      <!--<a-spin v-else size="large" style="margin-left: 45%;" />-->
    </a-modal>
    <!-- 表单区域 -->
    <mbp-mqtt-gateway-config-modal ref="modalForm" @ok="modalFormOk"/>

  </a-card>
</template>

<script>

  import { JeecgListMixin } from '@/mixins/JeecgListMixin'
  import MbpMqttGatewayConfigModal from './modules/MbpMqttGatewayConfigModal'
  import MbpMqttGatewayTopicsSubTable from './subTables/MbpMqttGatewayTopicsSubTable'
  import {filterMultiDictText} from '@/components/dict/JDictSelectUtil'
  import '@/assets/less/TableExpand.less'
  import JSuperQuery from '@/components/jeecg/JSuperQuery.vue'
  import { getAction,putAction } from '@/api/manage'
  import store from '@/store/'

  export default {
    name: 'MbpMqttGatewayConfigList',
    mixins: [JeecgListMixin],
    components: {
      MbpMqttGatewayConfigModal,
      MbpMqttGatewayTopicsSubTable,
      JSuperQuery
    },
    data() {
      return {
        // 弹窗
        dataModelVisible: false,
        modelFlag: false,
        websock: null,
        lockReconnect:false,
        mqttMssage: '',
        description: '网关配置信息列表管理页面',
        // 表头
        columns: [
          {
            title: '#',
            key: 'rowIndex',
            width: 60,
            align: 'center',
            customRender: (t, r, index) => parseInt(index) + 1
          },
          {
            title: '网关名称',
            align: 'center',
            dataIndex: 'gatewayName',
          },
          {
            title: '部门编号',
            align: 'center',
            dataIndex: 'deptId_dictText'
          },
          {
            title: 'host',
            align: 'center',
            dataIndex: 'mqttHost',
          },
          {
            title: '用户名',
            align: 'center',
            dataIndex: 'mqttUsername',
          },
          {
            title: '密码',
            align: 'center',
            dataIndex: 'mqttPassword',
          },
          {
            title: '客户端ID',
            align: 'center',
            dataIndex: 'mqttClientId',
          },
          {
            title: '备注',
            align: 'center',
            dataIndex: 'disp',
          },
          /*{
            title: '创建人',
            align: 'center',
            dataIndex: 'createBy',
          },
          {
            title: '创建时间',
            align: 'center',
            dataIndex: 'createTime',
          },
          {
            title: '修改人',
            align: 'center',
            dataIndex: 'updateBy',
          },
          {
            title: '修改时间',
            align: 'center',
            dataIndex: 'updateTime',
          },
          {
            title: '是否删除',
            align: 'center',
            dataIndex: 'delFlag_dictText'
          },*/
          {
            title: '操作',
            dataIndex: 'action',
            align: 'center',
            width:147,
            scopedSlots: { customRender: 'action' },
          },
        ],
        // 字典选项
        dictOptions: {},
        // 展开的行test
        expandedRowKeys: [],
        url: {
          list: '/mqtt/mbpMqttGatewayConfig/list',
          delete: '/mqtt/mbpMqttGatewayConfig/delete',
          deleteBatch: '/mqtt/mbpMqttGatewayConfig/deleteBatch',
          exportXlsUrl: '/mqtt/mbpMqttGatewayConfig/exportXls',
          importExcelUrl: '/mqtt/mbpMqttGatewayConfig/importExcel',
          start: "/mqtt/mbpMqttGatewayConfig/startSub", // 开启订阅
          stop: "/mqtt/mbpMqttGatewayConfig/stopSub" // 开启订阅
        },
        superFieldList:[],
      }
    },
    created() {
      this.getSuperFieldList();
    },
    computed: {
      importExcelUrl() {
        return window._CONFIG['domianURL'] + this.url.importExcelUrl
      }
    },
    methods: {
      // 开启订阅
      handleStart(recode) {
        getAction(this.url.start,recode).then((res)=>{
          if(res.success){
            this.$notification.success({
              message: '操作成功',
              description: res.result,
            });
            this.loadData()
          } else {
            this.$notification.warning({
              message: '操作失败',
              description: res.message,
            });
          }
        });
      },
      // 关闭订阅
      handleStop(recode) {
        getAction(this.url.stop,recode).then((res)=>{
          if(res.success){
            this.$notification.success({
              message: '操作成功',
              description: res.result,
            });
            this.loadData()
          } else {
            this.$notification.warning({
              message: '操作失败',
              description: res.message,
            });
          }
        });
      },
      initDictConfig() {
      },
      handleExpand(expanded, record) {
        this.expandedRowKeys = []
        if (expanded === true) {
          this.expandedRowKeys.push(record.id)
        }
      },
      getSuperFieldList(){
        let fieldList=[];
        fieldList.push({type:'string',value:'gatewayName',text:'网关名称',dictCode:''})
        fieldList.push({type:'sel_depart',value:'deptId',text:'部门编号'})
        fieldList.push({type:'string',value:'mqttHost',text:'host',dictCode:''})
        fieldList.push({type:'string',value:'mqttUsername',text:'用户名',dictCode:''})
        fieldList.push({type:'string',value:'mqttPassword',text:'密码',dictCode:''})
        fieldList.push({type:'string',value:'mqttClientId',text:'客户端ID',dictCode:''})
        fieldList.push({type:'string',value:'disp',text:'备注',dictCode:''})
        fieldList.push({type:'string',value:'createBy',text:'创建人',dictCode:''})
        fieldList.push({type:'date',value:'createTime',text:'创建时间'})
        fieldList.push({type:'string',value:'updateBy',text:'修改人',dictCode:''})
        fieldList.push({type:'date',value:'updateTime',text:'修改时间'})
        fieldList.push({type:'int',value:'delFlag',text:'是否删除',dictCode:'del_flag'})
        this.superFieldList = fieldList
      },
      //查看实时数据
      checkData(recode) {
        this.dataModelVisible = true
        this.initWebSocket()
       /* setTimeout(()=> {
          this.modelFlag = true
        }, 3000)*/
      },
      //关闭实时数据弹窗
      handleModelClose() {
        //this.websocketOnclose()
        this.websocketOnclose()
        this.mqttMssage = ''
        //this.modelFlag = false
        //this.dataModelVisible = false
      },

      initWebSocket: function () {
        // WebSocket与普通的请求所用协议有所不同，ws等同于http，wss等同于https
        //var userId = store.getters.userInfo.id;
        var userId = 'admin_mqtt';
        //var userId ='admin';
        var url = window._CONFIG['domianURL'].replace("https://","wss://").replace("http://","ws://")+"/websocket/"+userId;
        //console.log(url);
        this.websock = new WebSocket(url);
        this.websock.onopen = this.websocketOnopen;
        this.websock.onerror = this.websocketOnerror;
        this.websock.onmessage = this.websocketOnmessage;
        this.websock.onclose = this.websocketOnclose;
      },
      websocketOnopen: function () {
        this.$notification.success({
          message: '连接成功'
        });
        console.log("WebSocket连接成功");
        //心跳检测重置
        //this.heartCheck.reset().start();
      },
      websocketOnerror: function (e) {
        this.$notification.error({
          message: '连接失败'
        });
        console.log("WebSocket连接发生错误");
        this.reconnect();
      },
      websocketOnmessage: function (e) {
        //console.log("-----接收消息-------",e.data);
        var json = eval("(" + e.data + ")"); //解析对象
        this.mqttMssage = ''
        if (this.dataModelVisible) {
          this.$notification.success({
            message: '数据更新'
          });
          // 将实时数据格式化
          if (typeof json != 'string') {
            json = JSON.stringify(json, undefined, 2);
          }
          json = json.replace(/&/g, '&').replace(/</g, '<').replace(/>/g, '>');
          json = json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function(match) {
            var cls = 'number';
            if (/^"/.test(match)) {
              if (/:$/.test(match)) {
                cls = 'key';
              } else {
                cls = 'string';
              }
            } else if (/true|false/.test(match)) {
              cls = 'boolean';
            } else if (/null/.test(match)) {
              cls = 'null';
            }
            return '<span class="' + cls + '">' + match + '</span>';
          });
          this.mqttMssage = json
        }
        //this.mqttMssage = JSON.stringify(json, null, "\t")

        //alert(JSON.stringify(data))
        /*if(data.cmd == "topic"){
          //系统通知
          this.loadData();
        }else if(data.cmd == "user"){
          //用户消息
          this.loadData();
        }*/
        //心跳检测重置
        //this.heartCheck.reset().start();
      },
      websocketOnclose: function (e) {
        console.log("connection closed (" + e + ")");
        if(e){
          console.log("connection closed (" + e.code + ")");
        }
        this.reconnect();
      },
      /*openNotification (data) {
        var text = data.msgTxt;
        const key = `open${Date.now()}`;
        this.$notification.open({
          message: '消息提醒',
          placement:'bottomRight',
          description: text,
          key,
          btn: (h)=>{
            return h('a-button', {
              props: {
                type: 'primary',
                size: 'small',
              },
              on: {
                click: () => this.showDetail(key,data)
              }
            }, '查看详情')
          },
        });
      },*/
      reconnect() {
        var that = this;
        if(that.lockReconnect) return;
        that.lockReconnect = true;
        //没连接上会一直重连，设置延迟避免请求过多
        setTimeout(function () {
          this.$notification.warning({
            message: '尝试重连...'
          });
          console.info("尝试重连...");
          that.initWebSocket();
          that.lockReconnect = false;
        }, 5000);
      },


    }
  }
</script>
<style lang="less" scoped>
  @import '~@assets/less/common.less';

  // 格式化输出json样式 -- 没有生效
  #pre {outline: 1px solid #ccc; padding: 5px; margin: 5px; font-weight: normal; font-size: 18px; white-space: pre-wrap; word-wrap: break-word;  }
  /*#pre>.string { color: green; }
  #pre>.number { color: darkorange; }
  #pre>.boolean { color: blue; }
  #pre .null { color: magenta; }
  #pre>.key { color: red; }*/
</style>