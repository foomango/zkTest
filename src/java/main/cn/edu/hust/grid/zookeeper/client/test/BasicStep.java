package cn.edu.hust.grid.zookeeper.client.test;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;

public class BasicStep implements Watcher {

	private static final String CLIENT_PORT = "2181";
	private static final int CONNECTION_TIMEOUT = 3000;
	
	/*
	 * Whether zookeeper has been connected synchronously
	 */
	private static boolean zkSyncConnected = false;
	
	// 监控所有被触发的事件
	public void process(WatchedEvent event) {
		System.out.println("已经触发了" + event.getType() + "事件！");
		if (event.getType() == Event.EventType.None) {
			switch (event.getState()) {
			case SyncConnected: 
				synchronized (this) {
					zkSyncConnected = true;
					notifyAll();
				}
				break;
			case Expired: 
				synchronized (this) {
					zkSyncConnected = false;
				}
				break;
			default: 
				break;
			}
		}
	}
	
	public void test() throws IOException, InterruptedException, KeeperException {
		// 创建一个与服务器的连接
				ZooKeeper zk = new ZooKeeper("10.114.0.1:" + CLIENT_PORT,
						CONNECTION_TIMEOUT, this);
				
				// 等待zookeeper连接建立
				synchronized (this) {
					while (!zkSyncConnected) {
						wait();
					}
				}

				// 创建一个目录节点
				zk.create("/testRootPath", "testRootData".getBytes(),
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				// 创建一个子目录节点
				zk.create("/testRootPath/testChildPathOne",
						"testChildDataOne".getBytes(), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				System.out
						.println(new String(zk.getData("/testRootPath", false, null)));
				// 取出子目录节点列表
				System.out.println(zk.getChildren("/testRootPath", true));
				// 修改子目录节点数据
				zk.setData("/testRootPath/testChildPathOne",
						"modifyChildDataOne".getBytes(), -1);
				System.out.println("目录节点状态：[" + zk.exists("/testRootPath", true) + "]");
				// 创建另外一个子目录节点
				zk.create("/testRootPath/testChildPathTwo",
						"testChildDataTwo".getBytes(), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				System.out.println(new String(zk.getData(
						"/testRootPath/testChildPathTwo", true, null)));
				// 删除子目录节点
				zk.delete("/testRootPath/testChildPathTwo", -1);
				zk.delete("/testRootPath/testChildPathOne", -1);
				// 删除父目录节点
				zk.delete("/testRootPath", -1);
				
				// 创建一个ephemeral目录节点
				zk.create("/testEphemeral", "testEphemeralData".getBytes(), 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				System.out.println(new String(zk.getData("/testEphemeral", true, null)));
				// 关闭连接
				zk.close();
	}

	public static void main(String[] args) throws IOException, KeeperException,
			InterruptedException {
		BasicStep bs = new BasicStep();
		bs.test();
	}
}
