TODO:

 - verify the datanode hostname by checking if the master can connect to that IP.
   if that fails, revert to IP.
 - name node and job tracker only binds to the IP specified in HDFS URL
   -> this cannot be fixed