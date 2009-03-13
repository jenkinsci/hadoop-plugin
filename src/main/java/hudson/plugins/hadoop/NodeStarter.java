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

import hudson.FilePath;
import hudson.model.Node;
import hudson.model.TaskListener;
import hudson.remoting.Callable;
import hudson.remoting.Channel;
import hudson.util.IOException2;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;

/**
 * Runs in the slave JVM to start Hadoop JVM.
 *
 * <p>
 * Doing this from the slave JVM and not from the master JVM
 * simplifies establishing the connection with this JVM.
 */
class NodeStarter implements Callable<Void,IOException> {
    private final TaskListener listener;
    private final String hdfsUrl;
    private final String jobTrackerAddress;
    private final FilePath rootPath;

    public NodeStarter(Node n, TaskListener listener, String hdfsUrl, PluginImpl p) throws MalformedURLException {
        this.listener = listener;
        this.hdfsUrl = hdfsUrl;
        this.jobTrackerAddress = p.getJobTrackerAddress();
        this.rootPath = n.getRootPath();
    }

    @Override
    public Void call() throws IOException {
        try {
            Channel channel = PluginImpl.createHadoopVM(new File(rootPath.getRemote()), listener);
            channel.call(new DataNodeStartTask(hdfsUrl, rootPath.getRemote()));
            channel.call(new TaskTrackerStartTask(hdfsUrl, jobTrackerAddress, rootPath.getRemote()));
            return null;
        } catch (InterruptedException e) {
            throw new IOException2(e);
        }
    }

    private static final long serialVersionUID = 1L;
}
