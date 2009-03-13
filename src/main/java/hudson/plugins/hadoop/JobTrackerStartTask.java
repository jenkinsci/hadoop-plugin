/*
 * The MIT License
 *
 * Copyright (c) 2004-2009, Sun Microsystems, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package hudson.plugins.hadoop;

import hudson.remoting.Callable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;

import java.io.File;
import java.io.IOException;

/**
 * Starts a {@link JobTracker}.
 */
class JobTrackerStartTask implements Callable<Void,Exception>, Runnable {
    private final File hudsonRoot;
    private final String hdfsUrl;
    private final String jobTrackerAddress;

    JobTrackerStartTask(File hudsonRoot, String hdfsUrl, String jobTrackerAddress) {
        this.hudsonRoot = hudsonRoot;
        this.hdfsUrl = hdfsUrl;
        this.jobTrackerAddress = jobTrackerAddress;
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
//        Configuration conf = new Configuration();
        JobConf jc = new JobConf();
        jc.set("fs.default.name",hdfsUrl);
        jc.set("mapred.job.tracker",jobTrackerAddress);
        jc.set("mapred.job.tracker.http.address","0.0.0.0:"+HTTP_PORT);
        jc.set("mapred.local.dir",new File(hudsonRoot,"hadoop/mapred").getPath());
        tracker = JobTracker.startTracker(jc);

        new Thread(this).start();

        return null;
    }

    private static final long serialVersionUID = 1L;
    public static final int HTTP_PORT = 50030;
}
