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

import hudson.Extension;
import hudson.model.Computer;
import hudson.model.TaskListener;
import hudson.remoting.Callable;
import hudson.slaves.ComputerListener;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    public void onOnline(Computer c, TaskListener listener) {
        try {
            PluginImpl p = PluginImpl.get();
            String hdfsUrl = p.getHdfsUrl();
            if(hdfsUrl !=null) {
                String address = decideAddress(c);
                if(address==null)
                    listener.getLogger().println("Unable to determine the hostname/IP address of this system. Skipping Hadoop deployment");
                else
                    c.getChannel().call(new SlaveStartTask(c, listener, hdfsUrl, address));
            }
        } catch (IOException e) {
            e.printStackTrace(listener.error("Failed to start Hadoop"));
        } catch (InterruptedException e) {
            e.printStackTrace(listener.error("Failed to start Hadoop"));
        }
    }


    /**
     * Hadoop needs each node to have a name that's reachable by all the other nodes,
     * yet it's surprisingly tricky for a machine to know a name that other systems can get to,
     * especially between things like DNS search suffix, the hosts file, and YP.
     *
     * <p>
     * So the technique here is to compute possible interfaces and names on the slave,
     * then try to ping them from the master, and pick the one that worked.
     */
    private String decideAddress(Computer c) throws IOException, InterruptedException {
        for( String address : c.getChannel().call(new ListPossibleNames())) {
            try {
                InetAddress ia = InetAddress.getByName(address);
                if(ia.isReachable(500))
                    return ia.getCanonicalHostName();
            } catch (IOException e) {
                // if a given name fails to parse on this host, we get this error
                LOGGER.log(Level.FINE, "Failed to parse "+address,e);
            }
        }
        return null;
    }

    private static class ListPossibleNames implements Callable<List<String>,IOException> {
        public List<String> call() throws IOException {
            List<String> names = new ArrayList<String>();
            List<String> ips = new ArrayList<String>();
            InetAddress localHost = InetAddress.getLocalHost();
            if(!localHost.isLoopbackAddress())
                names.add(localHost.getCanonicalHostName());

            Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
            while (nis.hasMoreElements()) {
                NetworkInterface ni =  nis.nextElement();
                Enumeration<InetAddress> e = ni.getInetAddresses();
                while (e.hasMoreElements()) {
                    InetAddress ia =  e.nextElement();
                    if(ia.isLoopbackAddress())  continue;
                    names.add(ia.getCanonicalHostName());
                    ips.add(ia.getHostAddress());
                }
            }
            names.addAll(ips);
            return names;
        }
        private static final long serialVersionUID = 1L;
    }

    private static final Logger LOGGER = Logger.getLogger(ComputerListenerImpl.class.getName());
}
