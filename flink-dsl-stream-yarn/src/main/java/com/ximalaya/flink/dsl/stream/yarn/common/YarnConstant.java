package com.ximalaya.flink.dsl.stream.yarn.common;

/**
 * @author martin.dong
 **/

public class YarnConstant {

    private YarnConstant(){}

    /**
     * 指定用于执行作业的Yarn队列名字
     */
    public static String YARN_QUEUE = "yarn.queue";

    /**
     * 指定用于执行作业的所需要分配的ResourceManager数量与TaskManager数量一致
     */
    public static String YARN_CONTAINER_NUM = "yarn.container.num";

    /**
     * 指定在所有TaskManager中创建的TaskSlot总的数量
     */
    public static String YARN_SLOTS_NUM = "yarn.slots.num";

    /**
     * 指定生成一个TaskManager进程能够开辟的内存大小
     */
    public static String YARN_TASK_MANAGER_MEMORY = "yarn.task.manager.memory";

    /**
     * 指定生成一个JobManager进程能够开辟的内存大小
     */
    public static String YARN_JOB_MANAGER_MEMORY = "yarn.job.manager.memory";

    /**
     * 作业在Yarn上的名字
     */
    public static String YARN_NAME = "yarn.name";

    /**
     * 指定作业以detached模式运行
     */
    public static String YARN_DETACHED_MODE = "yarn.detached.mode";

    public static String YARN_SHIP_PATH = "yarn.ship.path";
}
