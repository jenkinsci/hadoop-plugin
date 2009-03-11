package hudson.plugins.hadoop;

import hudson.FilePath;
import hudson.Launcher.LocalLauncher;
import hudson.Plugin;
import hudson.Proc;
import hudson.Launcher;
import hudson.model.Computer;
import hudson.model.TaskListener;
import hudson.model.Hudson;
import hudson.remoting.Callable;
import hudson.remoting.Channel;
import hudson.remoting.Which;
import hudson.slaves.Channels;
import hudson.util.ArgumentListBuilder;
import hudson.util.StreamTaskListener;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.MalformedURLException;

/**
 * @author Kohsuke Kawaguchi
 */
public class PluginImpl extends Plugin {
    private Channel channel;
    @Override
    public void start() throws Exception {
        String hdfsUrl = getHdfsUrl();
        if(hdfsUrl!=null) {
            // start Hadoop namenode and tracker node
            StreamTaskListener listener = new StreamTaskListener(System.out);
            channel = createHadoopVM(listener, new LocalLauncher(listener));
            channel.call(new NameNodeStartTask(hdfsUrl));
            channel.call(new JobTrackerStartTask(hdfsUrl));
        }
    }

    /**
     * Determines the HDFS URL.
     */
    public String getHdfsUrl() throws MalformedURLException {
        // TODO: port should be configurable
        String rootUrl = Hudson.getInstance().getRootUrl();
        if(rootUrl==null)
            return null;
        URL url = new URL(rootUrl);
        return "hdfs://"+url.getHost()+":9000/";
    }

    /*package*/ Channel createHadoopVM(TaskListener listener, Launcher launcher) throws IOException, InterruptedException {
        // launch Hadoop in a new JVM and have them connect back to us
        ServerSocket serverSocket = new ServerSocket();
        serverSocket.bind(null);
        serverSocket.setSoTimeout(10*1000);

        ArgumentListBuilder args = new ArgumentListBuilder();
        args.add(new File(System.getProperty("java.home"),"bin/java"));
        args.add("-jar");
        args.add(Which.jarFile(Channel.class));

        // build up a classpath
        StringBuilder classpath = new StringBuilder();
        File hadoopHome = new File("/usr/local/hadoop-0.19.0");
        for( String mask : new String[]{"hadoop-*-core.jar","lib/**/*.jar"}) {
            for(FilePath jar : new FilePath(hadoopHome).list(mask)) {
                if(classpath.length()>0)    classpath.append(File.pathSeparatorChar);
                classpath.append(jar.getRemote());
            }
        }
        args.add("-cp").add(classpath);

        args.add("-connectTo","localhost:"+serverSocket.getLocalPort());

        Proc p = launcher.launch(args.toCommandArray(), new String[0], listener.getLogger(), null);

        Socket s = serverSocket.accept();
        serverSocket.close();

        return Channels.forProcess("Channel to Hadoop", Computer.threadPoolForRemoting,
                new BufferedInputStream(s.getInputStream()), new BufferedOutputStream(s.getOutputStream()), p);
    }

    @Override
    public void stop() throws Exception {
        channel.close();
    }

    /**
     * Starts a {@link NameNode}.
     */
    private static class NameNodeStartTask implements Callable<Void,Exception> {
        private final String hdfsUrl;

        private NameNodeStartTask(String hdfsUrl) {
            this.hdfsUrl = hdfsUrl;
        }

        public Void call() throws Exception {
            FileUtils.deleteDirectory(new File("/tmp/hadoop"));
            final Configuration conf = new Configuration();

            // location of the name node
            conf.set("fs.default.name",hdfsUrl);
            conf.set("dfs.http.address", "0.0.0.0:12301");
            // namespace node stores information here
            conf.set("dfs.name.dir","/tmp/hadoop/namedir");
            // dfs node stores information here
            conf.set("dfs.data.dir","/tmp/hadoop/datadir");

            conf.setInt("dfs.replication",1);

            System.out.println("Formatting HDFS");
            NameNode.format(conf);

            System.out.println("Starting namenode");
            NameNode.createNameNode(new String[0], conf);
            return null;
        }

        private static final long serialVersionUID = 1L;
    }

    /**
     * Starts a {@link JobTracker}.
     */
    private static class JobTrackerStartTask implements Callable<Void,Exception>, Runnable {
        private final String hdfsUrl;

        private JobTrackerStartTask(String hdfsUrl) {
            this.hdfsUrl = hdfsUrl;
        }

        private transient JobTracker tracker;

        public void run() {
            try {
                tracker.offerService();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public Void call() throws Exception {
            // JobTracker dies with NPE if we don't have this
            System.setProperty("hadoop.log.dir","/tmp/hadoop/log");

//        Configuration conf = new Configuration();
            JobConf jc = new JobConf();
            jc.set("fs.default.name",hdfsUrl);
            jc.set("mapred.job.tracker","localhost:22000");
            jc.set("mapred.job.tracker.http.address","0.0.0.0:22001");
            jc.set("mapred.local.dir","/tmp/hadoop/mapred");
            tracker = JobTracker.startTracker(jc);

            new Thread(this).start();

            return null;
        }

        private static final long serialVersionUID = 1L;
    }

    public static PluginImpl get() {
        return Hudson.getInstance().getPlugin(PluginImpl.class);
    }
}
