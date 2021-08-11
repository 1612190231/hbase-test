package com.luck.service.impl;

import com.luck.dao.impl.HBaseDaoImpl;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Query;
import org.geotools.data.*;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.hbase.data.HBaseDataStore;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;
import org.locationtech.geomesa.hbase.data.HBaseFeatureWriter;
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;

import java.io.IOException;
import java.util.*;

/**
 * @Author: gh
 * @Description: 通过geomesa操作hbase数据。
 * 1.新增：通过geomesa封装并存储数据到HBase。
 * //geomesa默认主键名称是__fid__
 * SimpleFeature封装的数据中，主键是feature ID(__fid__)，不是数据自身的ID(gid,fid等)。
 * 2.删除：根据rowkey删除数条记录。
 * 3.更新：更新除geom以外的字段。
 * 和新增使用同一个方法。
 * 4.查询：解析出所有字段并返回；分页；模糊查询。
 * 条件查询、模糊查询
 */
public class GeomesaHBaseImpl extends HBaseDaoImpl {


    public GeomesaHBaseImpl(DataBaseDTO dto){
        super(dto);
    }
    /**
     * 精确查询、模糊查询
     * @param tableName 表名
     * @param qualifier 字段名
     * @param fieldValue 字段值
     * @param pageSize 分页大小
     * @param pageNum 当前页码
     * @return
     */
    public Map<String, Object> queryByRandomField(String tableName, String qualifier, String fieldValue,
                                                  Integer pageSize, Integer pageNum) {
        Query query = null;
        DataStore ds = createDataStore(tableName);
        if(ds != null){
            //设置查询条件
            try {
                query = new Query(getTypeNames(ds)[0],
                        ECQL.toFilter(qualifier+" LIKE '%"+fieldValue+"%'"));
            } catch (CQLException e) {
                e.printStackTrace();
            }
        }
        return queryFeatureDatas(ds, query,pageSize,pageNum);
    }

    /**
     * 统计一个表中的记录总条数
     * 【注意：不能直接调用父类中的totalCount方法，统计结果不对！】
     * @param tableName 表名
     * @return 总条数
     */
    public long totalCount2(String tableName) {
        long count = 0;
        try {
            //不设置查询条件
            String indexTable = getXZ3IndexTable(tableName);
            //使用协处理器统计（任何一张）索引表才有结果。
            count = super.totalCount(indexTable);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
       /* DataStore ds = createDataStore(tableName);
        Query query = new Query(getTypeNames(ds)[0]);
        return getTotalCountByQuery(ds,query);*/
    }
    /**
     * 获取模糊查询
     * @param tableName 表名
     * @param qualifier 字段名
     * @param fieldValue 字段值
     * @return 模糊查询的结果总数
     */
    public long totalCountOfFuzzyQuery(String tableName, String qualifier, String fieldValue) {
        long count = 0;
        Query query = null;
        DataStore ds = createDataStore(tableName);
        if(ds != null){
            //设置查询条件
            try {
                query = new Query(getTypeNames(ds)[0],
                        ECQL.toFilter(qualifier+" LIKE '%"+fieldValue+"%'"));
            } catch (CQLException e) {
                e.printStackTrace();
            }
            count =  getTotalCountByQuery(ds, query);
        }
        return count;
    }
    /**
     * 分页查询。
     * @param tableName 表名
     * @param pageSize 每页的大小
     * @param pageNum  当前页的页数
     */
    public Map<String, Object> getAllRowsByPage(String tableName, Integer pageSize, Integer pageNum) {
        //不设置查询条件
        DataStore ds = createDataStore(tableName);
        Query query = null;
        if(ds != null){
            query = new Query(getTypeNames(ds)[0]);
        }
        return  queryFeatureDatas(ds, query,pageSize,pageNum);
    }
    /**
     * 获取指定命名空间（数据库）下所有的表的名称。
     * （排除元数据表）
     * 源码中createSchema时命名表：GeoMesaFeatureIndex
     * .formatTableName(ds.config.catalog, GeoMesaFeatureIndex.tableSuffix(this, partition), sft)
     * @param nameSpace
     * @return
     */
    @Override
    public List<String> getTablenamesOfDB(String nameSpace) {
        //过滤掉了disable的表
        List<String> fullTableNames = getFullTablenamesOfDB(nameSpace);
        List<String> filteredTableNames = new ArrayList<>();
        for (String fullName : fullTableNames) {
            List<String> cfs = getColumnFamilies(fullName);
            //catalog表的CF=m，索引表的CF=d
            if(cfs.contains("m")){
                fullName = fullName.contains(":") ? fullName.split(":")[1] : fullName;
                filteredTableNames.add(fullName);
            }
           /* DataStore ds = createDataStore(fullName);
        //当fullname是索引表时，datastore=null，此时报错：NoSuchColumnFamilyException/RemoteWithExtrasException。
            if(ds != null){
                fullName = fullName.contains(":") ? fullName.split(":")[1] : fullName;
                filteredTableNames.add(fullName);
            }*/
        }
        return filteredTableNames;
    }
    /**
     * 获取某张表的字段名称和字段类型。
     * @param tableName 表的名称
     * @return map：key字段名称，value字段类型的class字符串。
     */
    @Override
    public Map<String, String> getColumnNames(String tableName) {
        DataStore dataStore = createDataStore(tableName);
        return getColumnNames(dataStore);
    }

    /**
     * 新增/更新一条数据。
     * 【注意：新增的数据中不包括主键属性RowKey！主键的设置为后台自动生成！】
     * 【注意：更新的数据中要包括主键属性RowKey！】
     * @param tableName 表名
     * @param qualifiers 字段名
     * @param values 字段值
     * @return 插入的数据条数
     */
    public int insert(String tableName, String[]qualifiers, String[]values) {
        DataStore ds = createDataStore(tableName);
        if(ds != null){
            SimpleFeatureType sft = getSimpleFeatureType(ds);
            //组织数据
            if(qualifiers!=null && values!=null){
                int len1 = qualifiers.length;
                int len2 = values.length;
                if(len1==len2){
                    //查询表中的字段，确定主键
                /*Query query = new Query(getTypeNames(ds)[0]);
                Map<String, Object> queryMap = queryFeatureDatas(ds, query, 1, 1);
                String pk = queryMap.get("pk").toString();*/
                    //封装数据
                    List<Map<String, Object>> datas = new ArrayList<>();
                    Map<String, Object> map = new HashMap<>();
                    for(int i=0;i<len1;i++){
                        map.put(qualifiers[i], values[i]);
                    }
                    datas.add(map);
                    List<SimpleFeature> simpleFeatures = dataToSimpleFeatures(sft,datas);
                    return writeFeatures(ds, sft, simpleFeatures);
                }
            }
        }
        return 0;
    }

    /**
     * 根据指定的条件，删除一条或多条满足条件的数据。
     * 【注意：一般使用主键进行精确删除！！】
     * @param tableName
     * @param fieldValues 字段名称和值
     * @return 删除的数据条数
     */
    public int deleteRecords(String tableName, Map<String, Object> fieldValues) {
        int count = 0;
        DataStore ds = createDataStore(tableName);
        if(ds == null){
            return count;
        }
        if(fieldValues != null){
            List<Query> queries = new ArrayList<>();
            //获取全称的表名
            tableName = super.getFullTableName(tableName);
            //根据字段名称和值，构建查询条件
            Set<Map.Entry<String, Object>> entries = fieldValues.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                String field = entry.getKey();
                Object value = entry.getValue();
                try {
                    Filter filter = ECQL.toFilter(field + " = '" + value.toString() + "'");
                    queries.add(new Query(tableName, filter));
                } catch (CQLException e) {
                    e.printStackTrace();
                }
            }
            //根据查询条件，查询对应的features
            List<SimpleFeature> simpleFeatures = queryFeatures(ds, queries);
            //删除指定的features记录
            String typeName = getTypeNames(ds)[0];
            count = removeFeatures(ds, typeName, simpleFeatures);
        }
        return count;
    }

    /**
     * 查询某个命名空间下的若干个表的总计大小。
     * @param nameSpace 命名空间
     * @param tables 表的名称
     * @param unit 大小单位
     * @return 总计打下。
     */
    @Override
    public double storeSizeOfTbl(String nameSpace, String[] tables, SizeUnitEnum unit) {
        boolean flag = isNamespaceExist(nameSpace);
        List<String> allGivenTables = new ArrayList<>();
        if(flag && tables != null){
            for (String table : tables) {
                //搜索出指定tables相关的元数据表
                List<String> associatedMetaTables = getAssociatedMetaTables(table);
                allGivenTables.addAll(associatedMetaTables);
            }
        }
        String[] tableNamesArray = allGivenTables.toArray(new String[allGivenTables.size()]);
        return super.storeSizeOfTbl(nameSpace,tableNamesArray, unit);
    }
    private boolean checkTableEnabled(Connection conn, String tableName){
        try {
            String fullTableName = super.getFullTableName(tableName);
            TableName tn = TableName.valueOf(fullTableName);
            return conn.getAdmin().isTableEnabled(tn);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    /**
     * 删除若干条feature数据。
     * @param datastore
     * @param typeName feature type name
     * @param features 封装的需要删除的数据列表
     * @return 删除的条数
     */
    private int removeFeatures(DataStore datastore, String typeName, List<SimpleFeature> features){
        int count = 0;
        if(datastore == null){
            return count;
        }
        try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                     datastore.getFeatureWriter(typeName, Transaction.AUTO_COMMIT)) {
            HBaseFeatureWriter hbaseWriter = (HBaseFeatureWriter)writer;
            for (SimpleFeature feature : features) {
                hbaseWriter.removeFeature(feature);
                /* 以下方式行不通，数据并没有被删除：
                SimpleFeature next = writer.next();
                next = feature;
                writer.remove();*/
                //next.setAttributes(feature.getAttributes());
                count+=1;
            }
        }catch(IOException e){
            System.out.println(e.getMessage());
        }catch (NullPointerException e){
            System.out.println(e.getMessage());
        }
        System.out.println("Delete "+count+" features successfully!");
        return count;
    }
    /**
     * 写入feature数据
     * @param datastore
     * @param sft
     * @param features 封装的数据列表
     * @return 写入的条数
     */
    private int writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features){
        int count = 0;
        if(datastore == null){
            return count;
        }
        try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                     datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
            for (SimpleFeature feature : features) {
                // 使用geotools writer,获取一个feature，然后修改并提交。
                // appending writers will always return 'false' for haveNext, so we don't need to bother checking
                SimpleFeature toWrite = writer.next();
                // copy attributes:
                List<Object> attributes = feature.getAttributes();
                toWrite.setAttributes(attributes);
                // 如果设置了 feature ID, 需要转换为实现类，并添加USE_PROVIDED_FID hint到user data
                String featureId = feature.getID();
                ((FeatureIdImpl) toWrite.getIdentifier()).setID(featureId);
                //toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                // 或者可直接使用 PROVIDED_FID hint
                toWrite.getUserData().put(Hints.PROVIDED_FID, featureId);
                //如果没有设置feature ID, 会自动生成一个UUID
                toWrite.getUserData().putAll(feature.getUserData());
                // write the feature
                writer.write();
                count += 1;
            }
        }catch(IOException e){
            System.out.println(e.getMessage());
        }catch (NullPointerException e){
            System.out.println(e.getMessage());
        }
        System.out.println("Write "+count+" features successfully!");
        return count;
    }

    /**
     * 将数据封装到SimpleFeature，一行数据就是一个SimpleFeature。
     * @param sft feature类型信息
     * @param datas 多行数据值。map中的key是字段名，value是字段值。
     *              【注意数据中不包括主键！】
     * @return 多行数据组成的SimpleFeature对象列表。
     */
    private List<SimpleFeature> dataToSimpleFeatures(SimpleFeatureType sft,
                                                     List<Map<String, Object>> datas) {
        List<SimpleFeature> features = new ArrayList<>();
        //查看user data设置
        /*Map<Object, Object> userDatas = sft.getUserData();
        Boolean idProvided = (Boolean)userDatas.get(Hints.USE_PROVIDED_FID);
        idProvided = idProvided == null?Boolean.FALSE:idProvided;*/
        //使用geotools SimpleFeatureBuilder创建特征features
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
        for (Map<String, Object> rowMap : datas) {
            Set<String> fieldNames = rowMap.keySet();
            //设置feature id
            String featureId = null;
            if(fieldNames.contains(pk)){
                featureId = rowMap.get(pk).toString();
                fieldNames.remove(pk);
            }else{
                //新增时，自动生成feature id。
                featureId = UUID.randomUUID().toString();
            }
            SimpleFeature feature = null;
            for (String fieldName : fieldNames) {
                Object fieldValue = rowMap.get(fieldName);
                //写入数据
                builder.set(fieldName, fieldValue);
                //确定是否需要自己提供ID
               /* if(idProvided){
                    //使用自己提供的ID
                    featureId = UUID.randomUUID().toString();
                }*/
            }
            try {
                // 告知geotools，我们要使用自己提供的ID
                builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                // build the feature - this also resets the feature builder for the next entry
                // 一律使用自己提供的ID！
                feature = builder.buildFeature(featureId);
                features.add(feature);
            } catch (Exception e) {
                System.out.println("Invalid SimpleFeature data: " + e.toString());
            }
        }
        return Collections.unmodifiableList(features);
    }
    private Map<String, String> buildParams(String tableName) throws ParseException {
        System.out.println("Start building parameters...");
        String zk = dataBaseDTO.getIp()+":"+dataBaseDTO.getHost();
        String[]args = new String[]{"--hbase.zookeepers", zk,"--hbase.catalog",tableName};
        DataAccessFactory.Param[] infos = new HBaseDataStoreFactory().getParametersInfo();
        Options options = createOptions(infos);
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, args);
        return CommandLineDataStore.getDataStoreParams(command, options);
    }
    /**
     * 构建ECQL语句,根据字段类型来拼接ECQL。
     * @param qualifier  字段名
     * @param fieldValue 字段值
     * @return ECQL语句
     */
    private String buildEcqlPredicate(String tableName, String qualifier, String fieldValue){
        //获取全称的表名
        tableName = super.getFullTableName(tableName);
        Map<String, String> columns = getColumnNames(tableName);
        String type = "String";
        //strToLowerCase(STATE_NAME)
        Set<String> keys = columns.keySet();
        //确定给定的字段是什么类型
        for (String key : keys) {
            if(key.equals(qualifier)){
                type = columns.get(key);
            }
        }
        //判断如何拼接
        if(type.contains("String")){

        }
        return null;
    }
    private DataStore createDataStore(String tableName){
        DataStore ds = null;
        //获取全称的表名
        tableName = super.getFullTableName(tableName);
        if(checkTableEnabled(getConn(),tableName)){
            try {
                Map<String, String> params = buildParams(tableName);
                System.out.println("Loading datastore...");
                // use geotools service loading to get a datastore instance
                //当表禁用时datastore为null
                ds = DataStoreFinder.getDataStore(params);
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return ds;
    }
    private Options createOptions(DataAccessFactory.Param[] parameters) {
        // parse the data store parameters from the command line
        Options options = CommandLineDataStore.createOptions(parameters);
        return options;
    }
    /**
     * 获取某个表中的字段信息，包括：字段名、字段类型
     * @param dataStore  数据存储
     * @return 字段信息
     */
    private Map<String, String> getColumnNames(DataStore dataStore) {
        Map<String, String> columnNames = new HashMap<>();
        if(dataStore == null){
            return columnNames;
        }
        String typeName = getTypeNames(dataStore)[0];
        if(StringUtils.isEmpty(typeName)){
            throw new RuntimeException("feature type name should not be empty! ");
        }
        Query query = new Query(typeName);
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                     dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
            // loop through all results
            if(reader.hasNext()){
                //开始收集结果,读取一行的数据
                SimpleFeature feature = reader.next();
                //封装一行的数据
                Collection<Property> properties = feature.getProperties();
                for (Property p : properties) {
                    //获取字段类型
                    Class<?> binding = p.getType().getBinding();
                    String pType = binding.getName();
                    //获取字段名称
                    String pName = p.getName().toString();
                    columnNames.put(pName, pType);
                }
            }
        }catch(IOException e){
            System.out.println(e.getMessage());
        }catch (NullPointerException e){
            System.out.println(e.getMessage());
        }
        return columnNames;
    }
    private String[] getTypeNames(DataStore ds){
        String[] typeNames=new String[1];
        try {
            if(ds != null){
                String[] types = ds.getTypeNames();
                if(types != null && types.length > 0){
                    typeNames =  types;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return typeNames;
    }
    private SimpleFeatureType getSimpleFeatureType(DataStore ds){
        SimpleFeatureType sft = null;
        try {
            if(ds != null){
                String typeName = getTypeNames(ds)[0];
                sft = ds.getSchema(typeName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sft;
    }
    private Map<Object, Object> getUserData(DataStore ds) {
        SimpleFeatureType sft = getSimpleFeatureType(ds);
        if(sft != null){
            return sft.getUserData();
        }else{
            return new HashMap<>();
        }
    }
    private long getTotalCountByQuery(DataStore datastore,Query query){
        long count = 0;
        if(datastore == null){
            return count;
        }
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                     datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
            while(reader.hasNext()){
                //开始收集结果,读取一行的数据
                reader.next();
                count+=1;
            }
        }catch(IOException e){
            System.out.println(e.getMessage());
        }catch (NullPointerException e){
            System.out.println(e.getMessage());
        }
        return count;
    }
    private String getTableNameKey(String name, int version){
        //e.g."table.$name.v$version"
        return new StringBuilder("table")
                .append(".").append(name)
                .append(".").append("v").append(version)
                .toString();
    }
    /**
     * 获取与给定表相关的元数据表。
     * @param tableName
     * @return 表的名称列表
     */
    private List<String> getAssociatedMetaTables(String tableName){
        List<String> tables = new ArrayList<>();
        //获取全称的表名
        tableName = super.getFullTableName(tableName);
        tables.add(tableName);
        DataStore ds = createDataStore(tableName);
        if(ds != null){
            //转为HBaseDataStore，获取元数据
            HBaseDataStore hds = (HBaseDataStore)ds;
            GeoMesaMetadata<String> metadata = hds.metadata();
            //获取user data中的索引信息
            Map<Object, Object> userData = getUserData(ds);
            Object o = userData.get("geomesa.indices");
            if(o != null){
                //e.g."xz2:1:3,id:1:3"
                String indices = o.toString();
                String[] splits = indices.split(",");
                for (String split : splits) {
                    String[] oneIndiceInfo = split.split(":");
                    if(oneIndiceInfo.length>=3){
                        String tableNameKey = getTableNameKey(oneIndiceInfo[0], Integer.valueOf(oneIndiceInfo[1]));
                        String metaTableName = metadata.read(getTypeNames(ds)[0], tableNameKey, true).get();
                        if(!StringUtils.isBlank(metaTableName)){
                            tables.add(metaTableName);
                        }
                    }
                }
            }
        }
        return tables;
    }

    /**
     * 获取指定表的某个索引表的名称
     * @param tableName hbase表的全称：namespace:table
     * 空间索引（Z2/XZ2）、时间索引（Z3/XZ3）、ID索引、属性索引。
     * @return 索引表的名称
     */
    private String getXZ3IndexTable(String tableName) throws NoSuchElementException {
        //索引表//获取全称的表名
        String indexTable = super.getFullTableName(tableName);
        DataStore ds = createDataStore(tableName);
        if(ds != null){
            //转为HBaseDataStore，获取元数据
            HBaseDataStore hds = (HBaseDataStore)ds;
            GeoMesaMetadata<String> metadata = hds.metadata();
            //获取user data中的索引信息
            Map<Object, Object> userData = getUserData(ds);
            Object o = userData.get("geomesa.indices");
            if(o != null){
                //e.g."xz2:1:3,id:1:3"
                String indices = o.toString();
                String[] splits = indices.split(",");
                Map<String, String> indexMap = new HashMap<>();
                for (String split : splits) {
                    String[] oneIndiceInfo = split.split(":");
                    String indexName = oneIndiceInfo[0];
                    String indexVersion = oneIndiceInfo[1];
                    if(oneIndiceInfo.length>=3){
                        String tableNameKey = getTableNameKey(indexName, Integer.valueOf(indexVersion));
                        String metaTableName = metadata.read(getTypeNames(ds)[0], tableNameKey, true).get();
                        indexMap.put(indexName, metaTableName);
                    }
                }
                //取出索引表
                if(indexMap.get("xz3")!=null){
                    indexTable = indexMap.remove("xz3");
                }else if(indexMap.get("z3")!=null){
                    indexTable = indexMap.remove("z3");
                }else if(indexMap.get("xz2")!=null){
                    indexTable = indexMap.remove("xz2");
                }else if(indexMap.get("z2")!=null){
                    indexTable = indexMap.remove("z2");
                }else if(indexMap.get("id")!=null){
                    indexTable = indexMap.remove("id");
                }else{
                    if(indexMap.size()>0){
                        indexTable = indexMap.values().iterator().next();
                    }
                }
            }
        }
        return indexTable;
    }
    private List<String> getColumnFamilies(String tableName){
        List<String> list = new ArrayList<>();
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            //使用全称的表
            tableName = super.getFullTableName(tableName);
            HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor[] hcdArray = htd.getColumnFamilies();
            for (HColumnDescriptor hcd : hcdArray) {
                list.add(hcd.getNameAsString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }
    /**
     * 获取命名空间下全称的表名，格式为  namespace:tablename
     * @param nameSpace 命名空间名称，相当于数据库
     * @return 表的全称的列表
     */
    private List<String> getFullTablenamesOfDB(String nameSpace) {
        List<String> list = new ArrayList<>();
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            TableName[] tableNames = admin.listTableNamesByNamespace(nameSpace);
            for (TableName name : tableNames) {
                //判断table是不是enable的 //过滤系统表
                if(checkTableEnabled(admin,name) && !name.isSystemTable()){
                    String tableName = name.getNameAsString();
                    list.add(tableName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }
    /**
     * 查询数据：精确查询、模糊查询、分页查询。
     * 字段名称：property.getName()  或者 property.getType().getName()
     * 字段值： feature.getAttribute(property.getName()) 或者 property.getValue()
     * @param dataStore
     * @param query  查询条件
     * @throws IOException
     */
    private Map<String, Object> queryFeatureDatas(DataStore dataStore, Query query,
                                                  Integer pageSize, Integer pageNum){
        if (pageSize == null || pageSize < 1) {
            pageSize = 10;
        }
        if (pageNum == null || pageNum < 1) {
            pageNum = 1;
        }
        //封装结果数据
        Map<String, Object> rowDatas = new HashMap<>();
        List<Map<String, String>> datas = new ArrayList<>();
        Set<String> fields = new HashSet<>();
        if(dataStore == null){
            rowDatas.put("data", datas);
            rowDatas.put("fields", fields);
            rowDatas.put("pk", pk);
            return rowDatas;
        }
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                     dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
            // loop through all results
            long totalLoopTimes = pageSize * pageNum;
            long startFrom = pageSize*(pageNum-1)+1;
            for(long count=1;count<=totalLoopTimes;count++){
                if(reader.hasNext()){
                    //开始收集结果,读取一行的数据。feature:ScalaSimpleFeature
                    SimpleFeature feature = reader.next();
                    if(count >= startFrom){
                        //每行数据的主键值
                        String id = feature.getID();
                        //封装一行的数据
                        Map<String, String> oneRowData = new HashMap<>();
                        //先把唯一的feature ID装进去
                        oneRowData.put(pk, id);
                        Collection<Property> properties = feature.getProperties();
                        for (Property p : properties) {
                            //property: AttributeImpl
                            //AttributeImpl attrImpl = (AttributeImpl)p;
                            //获取字段名称
                            Name name = p.getName();
                            String pName = name.toString();
                            //获取字段值
                            Object attrObj = feature.getAttribute(name);
                            String pValue = attrObj==null?null:attrObj.toString();
                            oneRowData.put(pName, pValue);
                            // use geotools data utilities to get a printable string
                            // System.out.println(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
                        }
                        fields.addAll(oneRowData.keySet());
                        datas.add(oneRowData);
                    }
                }
            }
        }catch(IOException e){
            System.out.println(e.getMessage());
        }catch (NullPointerException e){
            System.out.println(e.getMessage());
        }
        rowDatas.put("data", datas);
        rowDatas.put("fields", fields);
        rowDatas.put("pk", pk);
        return rowDatas;
    }


    /**
     * 根据指定条件查询SimpleFeature对象。
     * @param dataStore
     * @param queries (多个)查询条件
     * @return SimpleFeature对象的列表
     */
    private List<SimpleFeature> queryFeatures(DataStore dataStore, List<Query> queries) {
        List<SimpleFeature> results = new ArrayList<>();
        if(dataStore == null){
            return results;
        }
        for (Query query : queries) {
            System.out.println("Running query " + ECQL.toCQL(query.getFilter()));
            if (query.getPropertyNames() != null) {
                System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
            }
            if (query.getSortBy() != null) {
                SortBy sort = query.getSortBy()[0];
                System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
            }
            // 提交查询，获取匹配查询条件的features，并遍历。
            // try语句确保reader可以关闭。
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                         dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                long  n = 0;
                while (reader.hasNext()) {
                    SimpleFeature feature = reader.next();
                    results.add(feature);
                    n+=1;
                }
                System.out.println("Returned " + n + " total queried features");
            }catch(IOException e){
                System.out.println(e.getMessage());
            }catch (NullPointerException e){
                System.out.println(e.getMessage());
            }
        }
        return results;
    }
}