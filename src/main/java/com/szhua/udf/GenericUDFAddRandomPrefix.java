package com.szhua.udf;

/**
 * @Author SZhua
 * @Date 2021/3/4
 * @Description: say st
 */

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Random;

@Description(name = "add_random_prefix",
        value = "_FUNC_(str) - Returns str with string plush an random str",
        extended = "Example:\n"
                + "  > SELECT _FUNC_('Facebook',10) FROM src LIMIT 1;\n" + "  '8_Facebook'")
public class GenericUDFAddRandomPrefix extends GenericUDF {

    private transient PrimitiveObjectInspectorConverter.StringConverter stringConverter;


    private transient PrimitiveObjectInspector.PrimitiveCategory returnType = PrimitiveObjectInspector.PrimitiveCategory.STRING;
    private transient GenericUDFUtils.StringHelper returnHelper;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "add_random_prefix requires 2 argument, got " + arguments.length);
        }

        PrimitiveObjectInspector argumentOI1 = (PrimitiveObjectInspector) arguments[0];
        PrimitiveObjectInspector argumentOI2 = (PrimitiveObjectInspector) arguments[1];

        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE || arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException(
                    "add_random_prefix only takes primitive types, got " + argumentOI1.getTypeName() +"/" + argumentOI2.getTypeName());
        }

        stringConverter = new PrimitiveObjectInspectorConverter.StringConverter(argumentOI1);
        ObjectInspector outputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        returnType = PrimitiveObjectInspector.PrimitiveCategory.STRING;
        returnHelper = new GenericUDFUtils.StringHelper(returnType);
        return outputOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String val = null;
        if (arguments[0] != null) {
            val = (String) stringConverter.convert(arguments[0].get());
        }
        Integer number =null ;
        if (arguments[1]!=null){
            number = Integer.valueOf(arguments[1].get().toString());
        }

        if (val == null) {
            return null;
        }
        if (number==null){
            return  null ;
        }
        number =new Random().nextInt(number);
        return returnHelper.setReturnValue(number+"_"+val);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("upper", children);
    }

}
