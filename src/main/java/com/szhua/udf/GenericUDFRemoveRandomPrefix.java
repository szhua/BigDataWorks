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

@Description(name = "remove_random_prefix",
        value = "_FUNC_(str) - Returns str with string remove an random str",
        extended = "Example:\n"
                + "  > SELECT _FUNC_('10_Facebook') FROM src LIMIT 1;\n" + "  'Facebook'")
public class GenericUDFRemoveRandomPrefix extends GenericUDF {

    private transient PrimitiveObjectInspector argumentOI;
    private transient PrimitiveObjectInspectorConverter.StringConverter stringConverter;


    private transient PrimitiveObjectInspector.PrimitiveCategory returnType = PrimitiveObjectInspector.PrimitiveCategory.STRING;
    private transient GenericUDFUtils.StringHelper returnHelper;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "remove_random_prefix requires 2 argument, got " + arguments.length);
        }

        argumentOI = (PrimitiveObjectInspector) arguments[0];
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE ) {
            throw new UDFArgumentException(
                    "remove_random_prefix only takes primitive types, got " + argumentOI.getTypeName() );
        }

        stringConverter = new PrimitiveObjectInspectorConverter.StringConverter(argumentOI);
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
        if (val == null) {
            return null;
        }
        if (!val.contains("_")){
            return val ;
        }
        if (val.indexOf("_")==0){
            return  val.substring(1);
        }
        return returnHelper.setReturnValue(val.split("_")[1]);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("upper", children);
    }

}
