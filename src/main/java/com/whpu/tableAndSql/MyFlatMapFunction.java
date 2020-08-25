package com.whpu.tableAndSql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;

// 自定义split函数
public class MyFlatMapFunction extends TableFunction<Row> {
//  定义函数；类型
    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING , Types.INT);
    }
//    执行拆分
    public void eval(String line) {
        Arrays.stream(line.split(" ")).forEach(word -> {
            Row row = new Row(2); //长度：单词和数字
            row.setField(0 , word);
            row.setField(1 , 1);
            collect(row);//输出 流
        });
    }
}
