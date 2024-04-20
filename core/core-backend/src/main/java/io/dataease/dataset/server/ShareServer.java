package io.dataease.dataset.server;

import com.github.xiaoymin.knife4j.annotations.ApiSupport;
import com.google.common.collect.Lists;
import io.dataease.api.chart.dto.ChartExtFilterDTO;
import io.dataease.api.chart.dto.ChartViewDTO;
import io.dataease.api.chart.request.ChartExtRequest;
import io.dataease.api.dataset.dto.SqlVariableDetails;
import io.dataease.api.dataset.union.DatasetGroupInfoDTO;
import io.dataease.api.dataset.union.model.SQLMeta;
import io.dataease.api.ds.vo.TableField;
import io.dataease.auth.DeApiPath;
import io.dataease.dataset.dto.DatasourceSchemaDTO;
import io.dataease.dataset.manage.DatasetGroupManage;
import io.dataease.dataset.manage.DatasetSQLManage;
import io.dataease.datasource.provider.CalciteProvider;
import io.dataease.datasource.request.DatasourceRequest;
import io.dataease.dto.dataset.DatasetTableFieldDTO;
import io.dataease.engine.sql.SQLProvider;
import io.dataease.engine.trans.Table2SQLObj;
import io.dataease.exception.DEException;
import io.dataease.i18n.Translator;
import io.dataease.model.BusiNodeRequest;
import io.dataease.model.BusiNodeVO;
import io.dataease.result.ResultCode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dataease.constant.AuthResourceEnum.DATASET;


@Tag(name = "数据共享接口")
@ApiSupport(order = 980)
@DeApiPath(value = "/share", rt = DATASET)
@RestController
@RequestMapping("share")
public class ShareServer {
    @Resource
    private DatasetGroupManage datasetGroupManage;

    @Resource
    private DatasetSQLManage datasetSQLManage;

    @Resource
    private CalciteProvider calciteProvider;

    private static final Logger logger = LoggerFactory.getLogger(ShareServer.class);

    @Operation(summary = "获取数据集清单")
    @RequestMapping(value = "getDsList", method = {RequestMethod.GET, RequestMethod.POST})
    public List<Map<String, Object>> getDatasetList(BusiNodeRequest request) {
        List<BusiNodeVO> mergeList = new ArrayList<>();
        List<BusiNodeVO> tree = datasetGroupManage.tree(request);
        for (BusiNodeVO vo : tree) {
            expandTree(vo, mergeList, 0);
        }
        List<Long> ids = mergeList.stream().map(BusiNodeVO::getId).toList();
        List<SqlVariableDetails> sqlParams = datasetGroupManage.getSqlParams(ids);
        // 按照 datasetGroupId 进行分组
        Map<Long, List<SqlVariableDetails>> groupedMap = sqlParams.stream()
                .collect(Collectors.groupingBy(SqlVariableDetails::getDatasetGroupId));
        List<Map<String, Object>> retList = new ArrayList<>();
        for (BusiNodeVO busiNodeVO : mergeList) {
            Long id = busiNodeVO.getId();
            Boolean leaf = busiNodeVO.getLeaf();
            String name = busiNodeVO.getName();
            Map<String, Object> dataSetMap = new LinkedHashMap<>();
            dataSetMap.put("id", String.valueOf(id));
            dataSetMap.put("leaf", String.valueOf(leaf));
            dataSetMap.put("name", name);
            dataSetMap.put("paramList", groupedMap.get(id));
            retList.add(dataSetMap);
        }
        return retList;
    }

    void expandTree(BusiNodeVO vo, List<BusiNodeVO> retList, int lvl) {
        if (!StringUtils.equals(vo.getName(), "root")) {
            String prefix = "";
            if (lvl > 1) {
                prefix = StringUtils.leftPad("┗", lvl);
            }
            vo.setName(prefix + vo.getName());
            retList.add(vo);
        }
        if (CollectionUtils.isNotEmpty(vo.getChildren())) {
            for (BusiNodeVO child : vo.getChildren()) {
                expandTree(child, retList, lvl + 1);
            }
        }
        vo.setChildren(new ArrayList<>());
    }

    @Operation(summary = "查询数据集数据")
    @PostMapping("queryDsData")
    public Object queryDsData(@RequestBody ShareReq req) throws Exception {
        Long tableId = req.getDatasetGroupId();

        ChartExtRequest chartExtRequest = new ChartExtRequest();
//        chartExtRequest.setGoPage(req.getPageNum());
//        chartExtRequest.setPageSize(req.getPageSize());

        chartExtRequest.setFilter(Lists.newArrayList());
        List<ChartExtFilterDTO> filterList = chartExtRequest.getFilter();
        if (ObjectUtils.isNotEmpty(req.getParams())) {
            List<SqlVariableDetails> sqlVariables = datasetGroupManage.getSqlParams(Collections.singletonList(tableId));
            Map<String, SqlVariableDetails> varMap = sqlVariables.stream().collect(Collectors.toMap(SqlVariableDetails::getVariableName, Function.identity(), (o1, o2) -> o1));
            req.getParams().forEach((paramKey, paramVal) -> {
                if (varMap.containsKey(paramKey)) {
                    ChartExtFilterDTO fdo = new ChartExtFilterDTO();
                    fdo.setOperator("eq");
                    List<String> vals = Lists.newArrayList();
                    vals.add(paramVal);
                    fdo.setValue(vals);
                    // SqlVariableDetails
                    fdo.setParameters(List.of(varMap.get(paramKey)));
                    filterList.add(fdo);
                }
            });
        }
        // 获取数据集信息
        DatasetGroupInfoDTO table = datasetGroupManage.get(req.getDatasetGroupId(), null);
        if (table == null) {
            DEException.throwException(ResultCode.DATA_IS_WRONG.code(), Translator.get("i18n_no_ds"));
        }

        // 获取dsMap,union sql
        Map<String, Object> sqlMap = datasetSQLManage.getUnionSQLForEdit(table, chartExtRequest);
        String sql = (String) sqlMap.get("sql");
        Map<Long, DatasourceSchemaDTO> dsMap = (Map<Long, DatasourceSchemaDTO>) sqlMap.get("dsMap");

        // 调用数据源的calcite获得data
        DatasourceRequest datasourceRequest = new DatasourceRequest();
        datasourceRequest.setDsList(dsMap);

        String querySql;
        String totalPageSql;
        if (ObjectUtils.isEmpty(dsMap)) {
            DEException.throwException(ResultCode.DATA_IS_WRONG.code(), Translator.get("i18n_datasource_delete"));
        }
        for (Map.Entry<Long, DatasourceSchemaDTO> next : dsMap.entrySet()) {
            DatasourceSchemaDTO ds = next.getValue();
            if (StringUtils.isNotEmpty(ds.getStatus()) && "Error".equalsIgnoreCase(ds.getStatus())) {
                DEException.throwException(ResultCode.DATA_IS_WRONG.code(), Translator.get("i18n_invalid_ds"));
            }
        }
        ShareResp resp;
        SQLMeta sqlMeta = new SQLMeta();
        Table2SQLObj.table2sqlobj(sqlMeta, null, "(" + sql + ")");
        String originSql = SQLProvider.createQuerySQL(sqlMeta, false, true, new ChartViewDTO());// 明细表强制加排序
        // 分页查询
        if (req.getNeedPage()) {
            String limit = ((req.getPageNum() != null && req.getPageSize() != null) ? " LIMIT " + req.getPageSize() + " OFFSET " + (req.getPageNum() - 1) * req.getPageSize() : "");
            querySql = originSql + limit;
            totalPageSql = "SELECT COUNT(*) FROM (" + originSql + ") COUNT_TEMP";
            logger.info("calcite api count sql: " + totalPageSql);
            datasourceRequest.setQuery(totalPageSql);
            datasourceRequest.setTotalPageFlag(true);
            List<String[]> tmpData = (List<String[]>) calciteProvider.fetchResultField(datasourceRequest).get("data");
            long total = ObjectUtils.isEmpty(tmpData) ? 0 : Long.parseLong(tmpData.get(0)[0]);
            long pages = (total / req.getPageSize()) + (total % req.getPageSize() > 0 ? 1 : 0);
            resp = new ShareRespPage();
            ((ShareRespPage) resp).setTotal(total);
            ((ShareRespPage) resp).setPages(pages);
            ((ShareRespPage) resp).setPageNum(req.getPageNum());
            ((ShareRespPage) resp).setPageSize(req.getPageSize());
        } else {
            resp = new ShareResp();
            querySql = originSql + " LIMIT " + req.getPageSize();
        }

        datasourceRequest.setQuery(querySql);
        logger.info("calcite api sql: " + querySql);
        Map<String, Object> dataResult = calciteProvider.fetchResultField(datasourceRequest);

        List<String[]> dataStrList = (List<String[]>) dataResult.get("data");
        List<TableField> tableFieldList = (List<TableField>) dataResult.get("fields");
        // 全部参数转fieldMap key-Field
        Map<String, DatasetTableFieldDTO> allFieldMap = table.getAllFields()
                .stream().collect(Collectors.toMap(DatasetTableFieldDTO::getDataeaseName, Function.identity(), (o1, o2) -> o1));
        List<Map<String, String>> respFieldMapList = new ArrayList<>();
        for (TableField tableField : tableFieldList) {
            String dataeaseName = tableField.getOriginName();
            DatasetTableFieldDTO datasetTableFieldDTO = allFieldMap.get(dataeaseName);
            Map<String, String> fieldMap = new HashMap<>();
            fieldMap.put("id", dataeaseName);
            fieldMap.put("name", datasetTableFieldDTO.getName());
            fieldMap.put("originName", datasetTableFieldDTO.getOriginName());
            respFieldMapList.add(fieldMap);
        }
        resp.setFieldList(respFieldMapList);
        List<Map<String, String>> respDataList = new ArrayList<>();
        for (String[] oneLine : dataStrList) {
            Map<String, String> dataMap = new HashMap<>();
            for (int i = 0; i < oneLine.length; i++) {
                String dataeaseName = tableFieldList.get(i).getOriginName();
                String val = oneLine[i];
                // 只录入第一个值
                dataMap.putIfAbsent(allFieldMap.get(dataeaseName).getOriginName(), val);
            }
            respDataList.add(dataMap);
        }
        resp.setDataList(respDataList);
        return resp;
    }

}

@EqualsAndHashCode(callSuper = true)
@Schema(description = "数据集查询结果")
@Data
class ShareRespPage extends ShareResp {
    Long pageNum = 1L;
    Long pageSize = 100L;
    Long total = 0L;
    Long pages = 0L;
}

@Schema(description = "数据集查询结果")
@Data
class ShareResp {
    // {columnKey1:、columnKey2:、columnKey3:},
    List<Map<String, String>> dataList;
    // {columnName、originName、columnKey},
    List<Map<String, String>> fieldList;
}

@Schema(description = "数据集共享API查询请求")
@Data
class ShareReq {
    @Schema(description = "数据源集合ID")
    @NotNull(message = "集合ID不可以为空")
    Long datasetGroupId;

    Map<String, String> params;

    Boolean needPage = false;

    Long pageNum = 1L;
    @Max(value = 1000, message = "分页大小最大为1000")
    Long pageSize = 1000L;
}