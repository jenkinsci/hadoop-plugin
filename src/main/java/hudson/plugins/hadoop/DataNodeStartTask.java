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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

import java.io.File;
import java.io.IOException;

/**
 * Starts a {@link DataNode}.
 */
class DataNodeStartTask extends SlaveTask {
    DataNodeStartTask(String hdfsUrl, String rootPath, String address) {
        super(hdfsUrl, rootPath, address);
    }

    public Void call() throws IOException {
        System.out.println("Starting data node");

        Configuration conf = new Configuration();
        conf.set("fs.default.name",hdfsUrl);
        conf.set("dfs.data.dir",new File(new File(rootPath),"hadoop/datanode").getAbsolutePath());
        conf.set("dfs.datanode.address", "0.0.0.0:0");
        conf.set("dfs.datanode.http.address", "0.0.0.0:0");
        conf.set("dfs.datanode.ipc.address", "0.0.0.0:0");
        conf.set("slave.host.name", slaveHostName);

        // TODO: make this configurable
        // make room for builds
        conf.setLong("dfs.datanode.du.reserved",10L*1024*1024*1024);

        DataNode dn = DataNode.instantiateDataNode(new String[0],conf);
        DataNode.runDatanodeDaemon(dn);

        return null;
    }

    private static final long serialVersionUID = 1L;
}
