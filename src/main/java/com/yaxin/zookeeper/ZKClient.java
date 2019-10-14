package com.yaxin.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ZKClient {

    private ZooKeeper zkCli;
    private static final String CONNECT_STRING = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private static final int SESSION_TIMEOUT = 2000;

    @Before
    public void before() throws IOException {
        //回调函数,异步和另外一个节点沟通
        //服务端默认的回调函数,当客户端回复消息后,服务端专门写的回复客户端的函数,此时服务端会一直执行其他任务
        zkCli = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, event -> {
            //new的时候自动会调用该函数
            System.out.println("默认回调函数");
        });
        //使用lambda表示代替这里的new Watcher() {public void process(WatchedEvent event) { }}
    }

    @Test
    public void ls() throws KeeperException, InterruptedException {
        //true代表调用自带的回调函数,也就是上面new Watch
        List<String> children = zkCli.getChildren("/", e -> {
            System.out.println("自定义回调函数");
        });

        System.out.println("======================================");
        for (String child : children) {
            System.out.println(child);
        }
        System.out.println("======================================");

        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void create() throws KeeperException, InterruptedException {
        //acl(access control list):访问控制列表,定义哪些主机/IP可以访问我现在创建的节点
        //普通的acl,open to everything
        //EPHEMERAL临时的,s是返回的路径
        String s = zkCli.create("/Idea", "Idea2018".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        //当此程序结束的时候,创建的临时的节点就会消失,因为是临时的,只在当前session有效
        System.out.println(s);
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void get() throws KeeperException, InterruptedException {
        byte[] data = zkCli.getData("/zzzz", true, new Stat());

        String string = Arrays.toString(data);

        System.out.println(string);
    }

    @Test
    public void set() throws KeeperException, InterruptedException {
        //version用来控制,有多人进行修改的时候,保证版本不会边
        Stat stat = zkCli.setData("/zzzz", "defabc".getBytes(), 0);
        System.out.println(stat.getDataLength());
    }

    @Test
    public void stat() throws KeeperException, InterruptedException {
        Stat exists = zkCli.exists("/Idea", false);
        if (exists == null) {
            System.out.println("节点不存在");
        } else {
            System.out.println(exists.getDataLength());
        }
    }

    @Test
    public void delete() throws KeeperException, InterruptedException {
        Stat exists = zkCli.exists("/zzzz", false);
        //先判断节点是否存在,存在再删除
        if (exists != null) {
            //第二个参数是版本号,防止删除错误的
            //乐观锁,防止并发写时候发生的错误,你删除的东西是不是你看到的东西
            zkCli.delete("/zzzz", exists.getVersion());
        }

    }

    /**
     * 实现循环注册处理
     */
    private void register() throws KeeperException, InterruptedException {
        byte[] data = zkCli.getData("/a", event -> {
            try {
                register();
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }, null);

        System.out.println(new String(data));
    }

    @Test
    public void testRegister() {
        try {
            register();
            Thread.sleep(Long.MAX_VALUE);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
