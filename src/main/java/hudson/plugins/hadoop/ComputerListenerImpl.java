package hudson.plugins.hadoop;

import hudson.Extension;
import hudson.model.Computer;
import hudson.remoting.Callable;
import hudson.remoting.Channel;
import hudson.slaves.ComputerListener;
import hudson.util.StreamTaskListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;

/**
 * When a new computer becomes online, starts a Hadoop data node and task tracker.
 *
 * <p>
 * This will be done on a separate JVM to allow administrators to control the JVM parameters better.
 * This JVM automatically kills itself when the slave JVM gets disconnected.
 *
 * @author Kohsuke Kawaguchi
 */
@Extension
public class ComputerListenerImpl extends ComputerListener {
    @Override
    public void onOnline(Computer c) {
        try {
            // TODO: shouldn't ComputerListener gets TaskListener?
            StreamTaskListener listener = new StreamTaskListener(System.out);
            Channel channel = PluginImpl.createHadoopVM(listener,c.getNode().createLauncher(listener));
            channel.call(new DataNodeStartTask(c.getNode().getRootPath().getRemote()));
            channel.call(new TaskTrackerStartTask(c.getNode().getRootPath().getRemote()));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Starts a {@link DataNode}.
     */
    private static class DataNodeStartTask implements Callable<Void,IOException> {
        private final String rootPath;

        private DataNodeStartTask(String rootPath) {
            this.rootPath = rootPath;
        }

        public Void call() throws IOException {
            System.out.println("Starting data node");

            Configuration conf = new Configuration();
            conf.set("fs.default.name","hdfs://localhost:12300/");  // TODO: where's HDFS?
            conf.set("dfs.data.dir",new File(new File(rootPath),"hadoop/datanode").getAbsolutePath());
            conf.set("dfs.datanode.address", "127.0.0.1:0");
            conf.set("dfs.datanode.http.address", "127.0.0.1:0");
            conf.set("dfs.datanode.ipc.address", "127.0.0.1:0");

            DataNode dn = DataNode.instantiateDataNode(new String[0],conf);
            DataNode.runDatanodeDaemon(dn);
            
            return null;
        }

        private static final long serialVersionUID = 1L;
    }

    /**
     * Starts a {@link TaskTracker}.
     */
    private static class TaskTrackerStartTask implements Callable<Void,IOException> {
        private final String rootPath;

        private TaskTrackerStartTask(String rootPath) {
            this.rootPath = rootPath;
        }

        public Void call() throws IOException {
            System.out.println("Starting data node");

            JobConf conf = new JobConf();
            conf.set("fs.default.name","hdfs://localhost:12300/"); // TODO: where's HDFS?
            conf.set("mapred.job.tracker","localhost:22000");
            conf.set("slave.host.name", "localhost"); // TODO
            conf.set("mapred.task.tracker.http.address","localhost:0");
            conf.set("mapred.task.tracker.report.address","localhost:0");
            conf.set("mapred.local.dir",new File(new File(rootPath),"hadoop/datanode").getAbsolutePath());

            new Thread(new TaskTracker(conf)).start();

            return null;
        }

        private static final long serialVersionUID = 1L;
    }
}
