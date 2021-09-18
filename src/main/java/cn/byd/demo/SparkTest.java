package cn.byd.demo;

import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.kudu.spark.kudu.KuduWriteOptions;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SparkTest {
    private static final String KUDU_MASTER = "master02-cdpdev-ic";
    private static final String FK_TABLE = "impala::default.old_fangke_v_p";
    private static final String REGION_TABLE = "impala::default.region";
    private static final String FK_RES_TABLE = "impala::default.old_res_p_t";

    private static final List<String> regNames = new ArrayList<String>(){
        {
            add("股份有限公司");
            add("有限公司");
            add("分公司");
            add("公司");
            add("股份");
            add("有限");
            add("代理");
            add("集团");
        }
    };

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("SparkOA")
                .getOrCreate();

        //从kudu读取省市的名称
        Dataset<Row> regionDS = spark.read()
                .option("kudu.master", KUDU_MASTER)
                .option("kudu.table", REGION_TABLE)
                .format("kudu")
                .load();

        List<Row> collectRows = regionDS.collectAsList();

        for(Row row : collectRows){
            String areaName = row.getString(0);
            String areaNameExt = "("+areaName+")";
            String areaNames = "（"+areaName+"）";
            regNames.add(areaName);
            regNames.add(areaNameExt);
            regNames.add(areaNames);
        }

        regNames.sort(((o1, o2) -> o2.length() - o1.length()));

        //将regNames通过广播变量广播出去
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Broadcast<List<String>> broadcast = sc.broadcast(regNames);

        //从kudu获取访客数据
        Dataset<Row> fkDS = spark.read()
                .option("kudu.master", KUDU_MASTER)
                .option("kudu.table", FK_TABLE)
                .format("kudu")
                .load();

        //过滤筛选出供应商，选择字段
        Dataset<Row> selectDS = fkDS.where(new Column("lfyy").equalTo("供应商"))
                .select(col("zjlx"), new Column("zjhm"), new Column("lfdw"));

        //根据证件类型和证件号码分组
        Dataset<Row> aggDS = selectDS
                .groupBy(new Column("zjlx"), new Column("zjhm"))
                .agg(functions.collect_set(new Column("lfdw")));

//        aggDS.show();

        //创建表的schema信息
        List<StructField> sField = new ArrayList();
        sField.add(DataTypes.createStructField("zjlx",DataTypes.StringType,false));
        sField.add(DataTypes.createStructField("zjhm",DataTypes.StringType,false));
        sField.add(DataTypes.createStructField("cnames",DataTypes.createArrayType(DataTypes.StringType),false));
        sField.add(DataTypes.createStructField("oldNames",DataTypes.StringType,false));

        StructType sType = DataTypes.createStructType(sField);


        Dataset<Row> mapDS = aggDS.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            List<String> regList = broadcast.value();
            List<Row> rows = new ArrayList();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                String zjlx = row.getString(0);
                String zjhm = row.getString(1);
                List<String> cNames = row.getList(2);
//
                HashSet<String> nameSet = new HashSet<>();

                //replace regName
                for(String cName : cNames){
                    for(String regName : regList){
                        cName = cName.replace(regName,"").toUpperCase();
                    }
                    if(StringUtils.isNotEmpty(cName)){
                        nameSet.add(cName);
                    }
                }

                List<String> newNames = new ArrayList();
                newNames.addAll(nameSet);

                //desc sort
                newNames.sort((o1,o2) -> o2.length() - o1.length());

                similarityJudge(newNames,nameSet,0.6);

                rows.add(RowFactory.create(zjlx,zjhm,nameSet.toArray(),StringUtils.join(cNames,";")));

            }
            return rows.iterator();
        }, RowEncoder.apply(sType));

        Dataset<Row> resDS = mapDS.where(functions.size(new Column("cnames")).gt(1))
                .select(new Column("zjlx"), new Column("zjhm"), functions.concat_ws(";", new Column("cNames")).alias("cnames"), new Column("oldnames"));

        resDS.show();
//         write to kudu
        KuduContext kuduContext = new KuduContext(KUDU_MASTER, spark.sparkContext());
        KuduWriteOptions options = new KuduWriteOptions(false, true, false, false, false);
        kuduContext.upsertRows(resDS, FK_RES_TABLE, options);

        //关闭
        spark.close();

    }

    private static void similarityJudge(List<String> cNames, Set<String> cNameSet, double similarity){
        for(int i=0 ;i<cNames.size();i++){
            String c1 = cNames.get(i);
            for(int j=i+1; j<cNames.size();j++){
                String c2 = cNames.get(j);
                double jaroWinklerDistance = StringUtils.getJaroWinklerDistance(c1, c2);
                if(jaroWinklerDistance >= similarity){
                    cNameSet.remove(c2);
                }
            }
        }

    }


}
