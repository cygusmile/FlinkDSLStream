package com.ximalaya.flink.dsl.stream.classloader;

import com.ximalaya.flink.dsl.stream.classloader.DslStreamClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/26
 **/

public class TestDslStreamClassLoader {

   // @Test
    public void test() throws Exception{

        Configuration configuration = new Configuration();

        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
        configuration.set("fs.default.name", "hdfs://192.168.60.38:8020");

        DslStreamClassLoader classLoader = new DslStreamClassLoader(configuration,new String[]{"/user/dong/flinkDemoExtend/fxql-extends.jar"},null);

        Class<?> clazz=classLoader.loadClass("com.ximalaya.fxql.yarn.extend.DemoClass");
        Object object=clazz.newInstance();
        System.out.println(clazz.getMethod("getName").invoke(object));
        assert clazz.getMethod("getName").invoke(object).equals("luxiClass");
    }
}
