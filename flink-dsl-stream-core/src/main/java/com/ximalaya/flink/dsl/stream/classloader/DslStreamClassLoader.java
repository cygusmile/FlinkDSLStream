package com.ximalaya.flink.dsl.stream.classloader;

import com.google.common.base.Preconditions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/26
 **/

public class DslStreamClassLoader extends URLClassLoader {
    private static Logger LOG = LoggerFactory.getLogger(DslStreamClassLoader.class);

    private static URL[] constructUrlsFromClasspath(Configuration configuration,String[] classPaths) throws IOException {

        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(classPaths);
        try {
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(configuration));
        }catch (Throwable e){
            LOG.debug("ignore error",e);
        }
        List<URL> urls = new ArrayList<>();
        FileSystem fileSystem = FileSystem.get(configuration);
        for (String classPath : classPaths) {
            FileStatus[] jars = fileSystem.listStatus(new Path(classPath));
            for (FileStatus jar : jars) {
                String name = jar.getPath().getName();
                if (name.endsWith(".jar") || name.endsWith(".JAR")) {
                    LOG.debug("resolve jar: {}",name);
                    urls.add(jar.getPath().toUri().toURL());
                }
            }
        }
        return urls.toArray(new URL[0]);
    }

    /**
     * construct dslStreamClassLoader
     * @param configuration hdfs configuration
     * @param classPaths classPaths on hdfs
     * @throws IOException error happen when visit hdfs
     */
    public DslStreamClassLoader(Configuration configuration,String[] classPaths,ClassLoader parent) throws IOException {
        super(constructUrlsFromClasspath(configuration,classPaths),parent);
    }

    public DslStreamClassLoader(Configuration configuration,String classPaths,ClassLoader parent) throws IOException{
        this(configuration,classPaths.split(";"),parent);
    }

}
