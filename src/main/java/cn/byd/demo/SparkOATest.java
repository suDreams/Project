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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * Created by huang.hai7 on 2021/8/12.
 */
public class SparkOATest {
    private static final String KUDU_MASTER = "master02-cdpdev-ic:7051";
    private static final String FK_TABLE = "impala::default.old_fangke_v_p";
    private static final String REGION_TABLE = "impala::default.region";
    private static final String FK_RES_TABLE = "impala::default.old_res_p";

    private static final List<String> regNames = new ArrayList<String>() {
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
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkOASample")
                .master("local")
                .getOrCreate();

        Dataset<Row> regionDS  = spark.read()
                .option("kudu.master", KUDU_MASTER)
                .option("kudu.table", REGION_TABLE)
                .format("kudu")
                .load();

        List<Row> collectRows = regionDS.collectAsList();

        for (Row row : collectRows) {
            String areaName = row.getString(0);
            String areaNameExt = "（" + areaName + "）";
            regNames.add(areaName);
            regNames.add(areaNameExt);
        }

        regNames.sort(((o1, o2) ->
                o2.length() - o1.length()
        ));

        System.out.println("Name Size: " + regNames.size());

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Broadcast<List<String>> broadcast = sc.broadcast(regNames);

        Dataset<Row> fkDS = spark.read()
                .option("kudu.master", KUDU_MASTER)
                .option("kudu.table", FK_TABLE)
                .format("kudu")
                .load();

        Dataset<Row> selectDS = fkDS.where(new Column("lfyy").equalTo("供应商")).select(
                new Column("zjlx"),
                new Column("zjhm"),
                new Column("lfdw"));

        Dataset<Row> aggDS = selectDS.groupBy(new Column("zjlx"), new Column("zjhm")).agg(functions.collect_set(new Column("lfdw")));

        List<StructField> sFields = new ArrayList<>();
        sFields.add(DataTypes.createStructField("zjlx", DataTypes.StringType, false));
        sFields.add(DataTypes.createStructField("zjhm", DataTypes.StringType, false));
        sFields.add(DataTypes.createStructField("cnames", DataTypes.createArrayType(DataTypes.StringType), false));
        sFields.add(DataTypes.createStructField("oldnames", DataTypes.StringType, false));

        StructType sType = DataTypes.createStructType(sFields);


        Dataset<Row> mapDS = aggDS.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            List<String> regNameList = broadcast.value();
            List<Row> rows = new ArrayList<>();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                String zjlx = row.getString(0);
                String zjhm = row.getString(1);
                List<String> cNames = row.getList(2);
                Set<String> newNameSet = new HashSet<>();
                // reg replace
                for (String cName : cNames) {
                    for (String regName : regNameList) {
                        cName = cName.replace(regName, "");
                    }
                    if (StringUtils.isNotEmpty(cName)) {
                        newNameSet.add(cName);
                    }
                }

                List<String> newNames = new ArrayList<>();
                newNames.addAll(newNameSet);
                // sort desc
                newNames.sort((o1, o2) -> o2.length() - o1.length());

                similarityJudge(newNames, newNameSet, 0.6);

                rows.add(RowFactory.create(zjlx, zjhm, newNameSet.toArray(), StringUtils.join(cNames, ";")));
            }
            return rows.iterator();
        }, RowEncoder.apply(sType));

        Dataset<Row> resDS = mapDS.where(functions.size(new Column("cnames")).gt(2)).select(new Column("zjlx"), new Column("zjhm"), functions.concat_ws(";",
                new Column("cNames")).alias("cnames"), new Column("oldnames"));

        // write to kudu
        KuduContext kuduContext = new KuduContext(KUDU_MASTER, spark.sparkContext());
        KuduWriteOptions options = new KuduWriteOptions(false, true, false, false, false);
        kuduContext.upsertRows(resDS, FK_RES_TABLE, options);

        // close
        spark.close();
    }

    private static void similarityJudge(List<String> cNames, Set<String> cNameSet, double similarity) {
        for (int i = 0; i < cNames.size(); i++) {
            String c1 = cNames.get(i);
            for (int j = i + 1; j < cNames.size(); j++) {
                String c2 = cNames.get(j);
                /*double jwDistance = StringUtils.getJaroWinklerDistance(c1, c2);
                if (jwDistance >= similarity) {
                    cNameSet.remove(c2);
                }*/
            }
        }
    }
}
