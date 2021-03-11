package com.szhua.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author SZhua
 * @Date 2021/3/4
 * @Description: say st
 */
@Description(name = "bin",
        value = "_FUNC_(n) - returns n in binary",
        extended = "n is a BIGINT. Returns NULL if n is NULL.\n"
                + "Example:\n" + "  > SELECT _FUNC_(13) FROM src LIMIT 1\n" + "  '1101'")
public class HelloUDF extends UDF {
    public String evaluate(String input){
        return  "Hello Leilei"+input ;
    }
}
