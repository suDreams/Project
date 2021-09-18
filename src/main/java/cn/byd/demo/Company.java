package cn.byd.demo;

import cn.afterturn.easypoi.excel.ExcelImportUtil;
import cn.afterturn.easypoi.excel.annotation.Excel;
import cn.afterturn.easypoi.excel.annotation.ExcelTarget;
import cn.afterturn.easypoi.excel.entity.ImportParams;
import org.apache.commons.collections.CollectionUtils;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


@ExcelTarget("company")
public class Company implements Serializable {

    @Excel(name="供应商",width = 43,orderNum = "1")
    private String name;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public static List<String> importCompany() {
        ImportParams importParams = new ImportParams();
        //表格标题行数,默认0
        importParams.setTitleRows(0);
        //表头行数,默认1
        importParams.setHeadRows(1);
        //导出数据 参数1:当如excel文件  参数2:导入对象的类型 参数3:导入参数配置
        List<Company> companyList = ExcelImportUtil.importExcel(new File("D:\\MyWork\\厂家名称汇总 .xlsx"), Company.class,importParams);
        List<String> arr = new ArrayList<>();
        if(!CollectionUtils.isEmpty(companyList))
        {
            for (Company company:companyList){
                arr.add(company.getName());
            }
        }
        return arr;

    }

}
