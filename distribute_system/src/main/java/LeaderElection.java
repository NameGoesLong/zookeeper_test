/*
Zookeeper client would create extra two threads during the process:
1. IO Thread -- handles connection between our app and server
IMPORTANT: If IO thread did not come back to ZooKeeper within the TIME_OUT, ZooKeeper
would regard the node as dead.

2. Event Thread -- collect all the events coming from the zookeeper server

Connect and handle events for zookeeper:
-- Connection: use SyncConnected to start connection
-- Disconnection: when received disconnection/expired event, create a way to handle it.

How to debug using the log:
- Use log4j.properties to set the logs
*/
import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class LeaderElection implements Watcher{
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    // The timeout for a node to be judged as dead
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;


    public static void main(String[] args) throws IOException, InterruptedException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.run();
        // After been woken up from wait(), the main thread would execute the close()
        leaderElection.close();
        System.out.println("Disconnected from Zookeeper, exiting application.");
    }

    // establish a new connection to ZooKeeper server
    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    // helper function that keeps the program running
    public void run() throws InterruptedException{
        synchronized(zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException{
        zooKeeper.close();
    }

    // here arg0 is the event we received from zooKeeper server
    // This is the handler for our program
    @Override
    public void process(WatchedEvent arg0) {
        switch(arg0.getType()){
            case None:
                if(arg0.getState() == Event.KeeperState.SyncConnected){
                    // If the connection to zooKeeper is still on
                    System.out.println("Successfully connect to Zookeeper");
                }else{
                    // handle the lost connection event when we disconnect from ZooKeeper server
                    // Wake up the main thread
                    synchronized(zooKeeper){
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
        }
        
    }
}
