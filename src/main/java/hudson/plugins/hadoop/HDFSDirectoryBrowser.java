package hudson.plugins.hadoop;

import hudson.model.DirectoryBrowserSupport;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsShell;
import org.kohsuke.stapler.HttpResponse;
import org.kohsuke.stapler.StaplerRequest;
import org.kohsuke.stapler.StaplerResponse;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.File;
import java.util.List;

/**
 * Renders HDFS directory like {@link DirectoryBrowserSupport}.
 *
 * @author Kohsuke Kawaguchi
 */
public class HDFSDirectoryBrowser implements HttpResponse {
    private final DFSClient dfs;

    public HDFSDirectoryBrowser() throws IOException {
        PluginImpl p = PluginImpl.get();
        this.dfs = p.createDFSClient();
    }

    private String getPath(StaplerRequest req) {
        String path = req.getRestOfPath();
        if(path.length()==0)
            path = "/";
        return path;
    }

    public void generateResponse(StaplerRequest req, StaplerResponse rsp, Object node) throws IOException, ServletException {
//        String target = getPath(req);
//        if (!dfs.exists(target)) {
//            rsp.sendError(SC_NOT_FOUND);
//            return;
//        }
//
//        PrintWriter out = rsp.getWriter();
//
//        if (!dfs.isDirectory(target)) { // a file
//            List<LocatedBlock> blocks =
//                    dfs.namenode.getBlockLocations(target, 0, 1).getLocatedBlocks();
//
//            LocatedBlock firstBlock = null;
//            DatanodeInfo[] locations = null;
//            if (blocks.size() > 0) {
//                firstBlock = blocks.get(0);
//                locations = firstBlock.getLocations();
//            }
//            if (locations == null || locations.length == 0) {
//                out.print("Empty file");
//            } else {
//                DatanodeInfo chosenNode = jspHelper.bestNode(firstBlock);
//                String fqdn = InetAddress.getByName(chosenNode.getHost()).
//                        getCanonicalHostName();
//                String datanodeAddr = chosenNode.getName();
//                int datanodePort = Integer.parseInt(
//                        datanodeAddr.substring(
//                                datanodeAddr.indexOf(':') + 1,
//                                datanodeAddr.length()));
//                String redirectLocation = "http://" + fqdn + ":" +
//                        chosenNode.getInfoPort() +
//                        "/browseBlock.jsp?blockId=" +
//                        firstBlock.getBlock().getBlockId() +
//                        "&blockSize=" + firstBlock.getBlock().getNumBytes() +
//                        "&genstamp=" + firstBlock.getBlock().getGenerationStamp() +
//                        "&filename=" + URLEncoder.encode(dir, "UTF-8") +
//                        "&datanodePort=" + datanodePort +
//                        "&namenodeInfoPort=" + namenodeInfoPort;
//                rsp.sendRedirect(redirectLocation);
//            }
//            return;
//        }
//        // directory
//          FileStatus[] files = dfs.listPaths(target);
//          //generate a table and dump the info
//          String [] headings = { "Name", "Type", "Size", "Replication",
//                                  "Block Size", "Modification Time",
//                                  "Permission", "Owner", "Group" };
//          out.print("<h3>Contents of directory ");
//          JspHelper.printPathWithLinks(dir, out, namenodeInfoPort);
//          out.print("</h3><hr>");
//          JspHelper.printGotoForm(out, namenodeInfoPort, dir);
//          out.print("<hr>");
//
//          File f = new File(dir);
//          String parent;
//          if ((parent = f.getParent()) != null)
//            out.print("<a href=\"" + req.getRequestURL() + "?dir=" + parent +
//                      "&namenodeInfoPort=" + namenodeInfoPort +
//                      "\">Go to parent directory</a><br>");
//
//          if (files == null || files.length == 0) {
//            out.print("Empty directory");
//          }
//          else {
//            jspHelper.addTableHeader(out);
//            int row=0;
//            jspHelper.addTableRow(out, headings, row++);
//            String cols [] = new String[headings.length];
//            for (int i = 0; i < files.length; i++) {
//              //Get the location of the first block of the file
//              if (files[i].getPath().toString().endsWith(".crc")) continue;
//              if (!files[i].isDir()) {
//                cols[1] = "file";
//                cols[2] = FsShell.byteDesc(files[i].getLen());
//                cols[3] = Short.toString(files[i].getReplication());
//                cols[4] = FsShell.byteDesc(files[i].getBlockSize());
//              }
//              else {
//                cols[1] = "dir";
//                cols[2] = "";
//                cols[3] = "";
//                cols[4] = "";
//              }
//              String datanodeUrl = req.getRequestURL()+"?dir="+
//                  URLEncoder.encode(files[i].getPath().toString(), "UTF-8") +
//                  "&namenodeInfoPort=" + namenodeInfoPort;
//              cols[0] = "<a href=\""+datanodeUrl+"\">"+files[i].getPath().getName()+"</a>";
//              cols[5] = FsShell.dateForm.format(new Date((files[i].getModificationTime())));
//              cols[6] = files[i].getPermission().toString();
//              cols[7] = files[i].getOwner();
//              cols[8] = files[i].getGroup();
//              jspHelper.addTableRow(out, cols, row++);
//            }
//            jspHelper.addTableFooter(out);
//          }
//
//        String namenodeHost = jspHelper.nameNodeAddr.getHostName();
//        out.print("<br><a href=\"http://" +
//                  InetAddress.getByName(namenodeHost).getCanonicalHostName() + ":" +
//                  namenodeInfoPort + "/dfshealth.jsp\">Go back to DFS home</a>");
//        dfs.close();
    }
}
