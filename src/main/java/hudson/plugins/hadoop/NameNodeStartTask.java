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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.File;
import java.io.IOException;

/**
 * Starts a {@link NameNode}.
 */
class NameNodeStartTask implements Callable<Void,IOException> {
    private final File hudsonRoot;
    private final String hdfsUrl;
    private boolean format = Boolean.getBoolean("hadoop.format");

    NameNodeStartTask(File hudsonRoot, String hdfsUrl) {
        this.hudsonRoot = hudsonRoot;
        this.hdfsUrl = hdfsUrl;
    }

    public Void call() throws IOException {
        File hadoopRoot = new File(hudsonRoot,"hadoop");
        if(hadoopRoot.mkdirs())
            format = true;

        final Configuration conf = new Configuration();
        // location of the name node
        conf.set("fs.default.name",hdfsUrl);
        conf.set("dfs.http.address", "0.0.0.0:"+HTTP_PORT);
        // namespace node stores information here
        File namedir = new File(hadoopRoot, "namedir");
        if(namedir.mkdirs())
            format = true;
        conf.set("dfs.name.dir", namedir.getPath());
        // dfs node stores information here
        File datadir = new File(hadoopRoot, "datadir");
        if(datadir.mkdirs())
            format = true;
        conf.set("dfs.data.dir", datadir.getPath());

        conf.setInt("dfs.replication",1);

        if(format) {
            System.out.println("Formatting HDFS");
            NameNode.format(conf);
        }

        System.out.println("Starting namenode");
        NameNode.createNameNode(new String[0], conf);
        return null;
    }

    private static final long serialVersionUID = 1L;

    public static final int HTTP_PORT = 50070;
}
