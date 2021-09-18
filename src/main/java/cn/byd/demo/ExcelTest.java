package cn.byd.demo;

import cn.afterturn.easypoi.excel.ExcelImportUtil;
import cn.afterturn.easypoi.excel.entity.ImportParams;
import org.apache.commons.collections.CollectionUtils;

import java.io.File;
import java.util.List;

public class ExcelTest {
    public static void main(String[] args) {
        ImportParams importParams = new ImportParams();
        //表格标题行数,默认0
        importParams.setTitleRows(0);
        //表头行数,默认1
        importParams.setHeadRows(1);
        //导出数据 参数1:当如excel文件  参数2:导入对象的类型 参数3:导入参数配置
        List<Company> CompanyList = ExcelImportUtil.importExcel(new File("D:\\MyWork\\厂家名称汇总 .xlsx"), Company.class,importParams);
        if(!CollectionUtils.isEmpty(CompanyList))
            CompanyList.forEach(user->{
                System.out.println(user);
            });
    }
}
