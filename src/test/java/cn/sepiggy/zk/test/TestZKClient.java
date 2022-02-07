package cn.sepiggy.zk.test;

import cn.sepiggy.zk.entity.User;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class TestZKClient {

    private ZkClient zkClient;

    // 1. 创建节点
    @Test
    public void testCreateNode() {
        // 1) 持久节点
        String node1 = zkClient.create("/zk-baizhi/node1", "sepiggy", CreateMode.PERSISTENT);
        System.out.println("node1 = " + node1);
        // 2) 持久顺序节点
        String name = zkClient.create("/zk-baizhi/node1/name", "jms", CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println("name = " + name);
        // 3) 临时节点
        String age = zkClient.create("/zk-baizhi/node1/age", "100", CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("age = " + age);
        // 4) 临时顺序节点
        String height = zkClient.create("/zk-baizhi/node1/height", "193", CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("height = " + height);
    }

    // 2. 删除节点
    @Test
    public void testDeleteNode() {
        // 删除没有子节点的节点, 返回值: 是否删除成功
//        boolean delete = zkClient.delete("/zk-baizhi/node1");
//        System.out.println("delete = " + delete);

        //! 递归删除节点信息, 返回值: 是否删除成功
        boolean recursive = zkClient.deleteRecursive("/zk-baizhi/node1");
        System.out.println("recursive = " + recursive);
    }

    // 3. 查询当前节点下所有子节点
    @Test
    public void testFindNodes() {
        // 获取指定节点的子节点信息
        // 参数: 当前节点的路径
        // 返回值: 当前节点的子节点信息
        List<String> children = zkClient.getChildren("/");
        for (String child : children) {
            System.out.println(child);
        }
    }

    // 4. 获取某个节点数据
    //! 注意: 通过Java客户端操作需要保证节点存储的数据和获取节点时数据序列化方式一致
    //! 不要在CLI客户端设置值，然后通过Java客户端获取值，两者序列化方式不同，会报错
    //! 前者使用字符串方式序列化，后者通过JDK方式序列化
    //! 否则会抛出如下异常：
    //! org.I0Itec.zkclient.exception.ZkMarshallingError: java.io.StreamCorruptedException
    @Test
    public void testFindNodeData() {
        Object readData = zkClient.readData("/zk-baizhi/node1");
        System.out.println(readData);
    }

    // 5. 获取节点状态信息
    @Test
    public void testFindNodeDataAndStat() {
        Stat stat = new Stat();
        Object readData = zkClient.readData("/zk-baizhi/node1", stat);
        System.out.println(readData);
        System.out.println(stat.getCversion());
        System.out.println(stat.getCtime());
        System.out.println(stat.getCzxid());
    }

    // 6. 修改节点数据
    @Test
    public void testWriteData() {
        User user = new User();
        user.setId(1);
        user.setName("sepiggy");
        user.setAge(101);
        user.setBir(new Date());
        zkClient.writeData("/zk-baizhi/node1", user);
        user = zkClient.readData("/zk-baizhi/node1");
        System.out.println("user = " + user);
    }

    // 7. 监听节点数据变化
    //! 注意:
    //! 1) Java客户端的监听不同于使用CLI客户端的监听，使用Java客户端的监听是永久监听！
    //! 2) 使用Java客户端只能监听到使用Java客户端的修改操作
    @Test
    public void testWatchDataChange() throws IOException {
        System.out.println("开始监听当前节点...");
        zkClient.subscribeDataChanges("/zk-baizhi/node1", new IZkDataListener() {
            // 当前节点数据变化时触发对应这个方法
            public void handleDataChange(String dataPath, Object data) throws Exception {
                System.out.println("当前节点数据发生变化...");
                System.out.println("当前节点路径: " + dataPath);
                System.out.println("当前节点变化后数据: " + data);
            }

            // 当前节点删除时触发这个方法
            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println("当前节点被删除...");
                System.out.println("当前节点路径: " + dataPath);
            }
        });
        System.in.read(); // 阻塞当前线程，一直保持监听
    }

    // 8. 监听节点目录变化
    //! 注意：
    //! 1) Java客户端的监听不同于使用CLI客户端的监听，使用Java客户端的监听是永久监听！
    //! 2) 使用Java客户端只能监听到使用Java客户端的修改操作
    //! 3) 只能监听一级子目录
    @Test
    public void testOnNodesChange() throws IOException {
        zkClient.subscribeChildChanges("/zk-baizhi/node1", new IZkChildListener() {
            // 当节点的发生变化时,会自动调用这个方法
            // 参数1: 父节点名称
            // 参数2: 父节点中的所有子节点名称
            public void handleChildChange(String nodeName, List<String> list) throws Exception {
                System.out.println("当前节点目录(子节点)发生变化...");
                System.out.println("父节点名称: " + nodeName);
                System.out.println("发生变更后字节孩子节点名称:");
                for (String name : list) {
                    System.out.println(name);
                }
            }
        });
        System.in.read();
    }

    // 初始化客户端对象
    @Before
    public void before() {
        // 参数1: zk-server服务器ip地址:端口号
        // 参数2: 会话超时时间 (毫秒)
        // 参数3: 连接超时时间 (毫秒)
        // 参数4: 序列化方式  对象
        zkClient = new ZkClient("localhost:2181", 60000 * 30, 60000, new SerializableSerializer());
    }

    // 释放资源
    @After
    public void after() throws InterruptedException {
//        Thread.sleep(1000 * 60);
        zkClient.close();
    }
}
