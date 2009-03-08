package hudson.plugins.hadoop;

import hudson.Extension;
import hudson.FilePath.FileCallable;
import hudson.model.Computer;
import hudson.remoting.VirtualChannel;
import hudson.slaves.ComputerListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

import java.io.File;
import java.io.IOException;

/**
 * @author Kohsuke Kawaguchi
 */
@Extension
public class ComputerListenerImpl extends ComputerListener {
    @Override
    public void onOnline(Computer c) {
        try {
            c.getNode().getRootPath().child("hadoop").act(new DataNodeStartTask());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Starts a {@link DataNode}.
     */
    private static class DataNodeStartTask implements FileCallable<Void> {
        public Void invoke(File dir, VirtualChannel channel) throws IOException {
            Configuration conf = new Configuration();
            conf.set("fs.default.name","hdfs://localhost:12300/");
            conf.set("dfs.data.dir",dir.getAbsolutePath());
            conf.set("dfs.datanode.address", "127.0.0.1:0");
            conf.set("dfs.datanode.http.address", "127.0.0.1:0");
            conf.set("dfs.datanode.ipc.address", "127.0.0.1:0");

            System.out.println("Starting data node");
            DataNode dn = DataNode.instantiateDataNode(new String[0],conf);
            DataNode.runDatanodeDaemon(dn);
            
            return null;
        }

        private static final long serialVersionUID = 1L;
    }
}
