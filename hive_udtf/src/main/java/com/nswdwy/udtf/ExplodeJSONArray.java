package com.nswdwy.udtf;




import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yycstart
 * @create 2020-11-09 9:41
 */
public class ExplodeJSONArray extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //1. 约束函数传入参数个数
        List<? extends StructField> allStructList = argOIs.getAllStructFieldRefs();
        if(allStructList.size() != 1){
            throw new UDFArgumentException("explode_json_array函数的参数个数只能为1...");
        }
        //2.约束函数传入参数的类型
        StructField structField = allStructList.get(0);
        ObjectInspector fieldObjectInspector = structField.getFieldObjectInspector();
        String typeName = fieldObjectInspector.getTypeName();
        //hive中javaobjectFactory中String为全小写
        if(!"string".equals(typeName)){
            throw new UDFArgumentTypeException(0,"explode_json_array函数的第一个参数类型只能为string...");
        }
        //3.约束函数返回值类型

        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldIos = new ArrayList<>();
        fieldNames.add("aaa");
        fieldIos.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldIos);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        //1.从函数传参数中取得传入的jsonArrayStr
        String jsonArrayStr = args[0].toString();
        //2.
        JSONArray jsonArray = new JSONArray(jsonArrayStr);
        //3.将jsonArray中的json一个个写出
        for (int i = 0; i < jsonArray.length(); i++) {
            String ll = jsonArray.getString(i);
            String[] result =  new String[1];
            result[0] = ll;
            forward(result);
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
