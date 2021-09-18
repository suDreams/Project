package cn.byd.hana;

import cn.afterturn.easypoi.excel.entity.ImportParams;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Test {
    private static final List<String> company = new ArrayList(){
        {
            add("上海汇众汽车制造有限公司");
            add("广东恒勃滤清器有限公司");
            add("宁波均胜科技有限公司");
            add("山东美晨工业集团有限公司");
            add("芜湖众力部件有限公司");
        }
    };
    public static void main(String[] args) {

        List<String> test = new ArrayList();
        test.add("汇众汽车制造");
        test.add("美晨工程");
        test.add("美晨工业");
        test.add("恒勃滤清器");

        HashSet<String> newName = new HashSet<>();
        for (String a : test){
            for (String com : company){
                if(com.contains(a)){
                    a = a.replace(a,com);
                }
            }
            newName.add(a);
        }
        System.out.println(newName.toString());
    }



    private static void similarityJudge(List<String> cNames, Set<String> cNameSet, double similarity) {
        for (int i = 0; i < cNames.size(); i++) {
            String c1 = cNames.get(i);
            for (int j = i + 1; j < cNames.size(); j++) {
                String c2 = cNames.get(j);
                double jwDistance = StringUtils.getJaroWinklerDistance(c1, c2);
                if (jwDistance >= similarity) {
                    cNameSet.remove(c2);
                }
            }
        }
    }
}
