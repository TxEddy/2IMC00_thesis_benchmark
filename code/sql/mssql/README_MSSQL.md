## Microsoft SQL Server using Docker
Microsoft SQL Server on macOS could only be installed using Docker and the Microsoft SQL Server image. The Microsoft SQL Server image could be download from Microsoft itself as well as the installation instructions.

### Copying table data to container
* Connect to the container using the `docker` cli.
<pre>docker exec -it <i>CONTAINER_NAME</i> /bin/sh</pre>
* Create a new folder.
* Copy the tables files to the new folder created above to the container using the command:
    * The _/._ command will copy all files inside that folder.
<pre>docker cp <i>PATH_TO_TABLE_FILES</i>/. CONTAINER_NAME:<i>/PATH_TO/FOLDER_NAME_STEP1</i></pre>

#### Extra info
Sending command to the container could be done using the command:
<pre>docker exec -u 0 <i>CONTAINER_NAME</i> <i>COMMAND_TO_EXECUTE</i></pre>
e.g. executing a command to remove the folder and underlying files located in the `tmp` directory:
<pre>docker exec -u 0 <i>CONTAINIER_NAME</i> rm -rf /tmp/synthetic_files</pre>
