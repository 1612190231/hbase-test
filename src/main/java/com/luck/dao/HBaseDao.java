package com.luck.dao;

import org.apache.hadoop.hbase.client.Result;

import java.util.List;
import java.util.Map;

/**
 * @author luchengkai
 * @description Hbase数据查询
 * @date 2021/8/11 19:27
 */
public interface HBaseDao {

    void createTable(String tableName, String[] columnFamilys);

    int insert(String tableName, String rowKey, String family, String quailifer[], String value[]);

    List<Result> getAllRows(String tableName, Integer pageSize, Integer pageNum);

    long totalCount(String tableName);

    Result getRowByKey(String tableName, String rowKey);

    List<Result> getRowsByKey(String tableName, String rowKeyLike);

    List<Result> getRowsByKey(String tableName, String startRow, String stopRow);

    List<Result> getRowsByKey(String tableName, String rowKeyLike, String family, String cols[]);

    int deleteRecords(String tableName, String rowKeyLike);

    int deleteRecord(String tableName, String rowKey);

    Map<String, List<String>> getNameSpaceTables();

    List<String> getTablenamesOfDB(String nameSpace);

    double usedRate();

    double storeSizeOfDB(SizeUnitEnum unit);

    double storeSizeOfDB(String nameSpace, SizeUnitEnum unit);

    double storeSizeOfTbl(String nameSpace,String[] tables, SizeUnitEnum unit);

    int countTables(String nameSpace);

    List<String> getTables(String nameSpace);

    List<Result> queryByRandomField(String tableName, String familyName,String qualifier,
                                    String fieldValue,Integer pageSize,Integer pageNum);

    long totalCountOfFuzzyQuery(String tableName,String familyName,
                                String qualifier,String fieldValue);

    Map<String, String> getColumnNames(String tableName);


}
