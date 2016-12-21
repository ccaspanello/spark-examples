package com.github.ccaspanello.spark.engine.io;

import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemManager;
import org.apache.commons.vfs.VFS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.stream.Stream;

/**
 * Abstraction of the File System
 *
 * TODO Discuss a better design pattern.
 * VFS only supports Read-Only access to HDFS, how can we design this without a plethora of IF statements.  Also how can
 * we support multiple Hadoop Configuration easily.
 *
 * Created by ccaspanello on 12/19/2016.
 */
public class IOHandler implements Serializable {

    private Configuration hadoopConfig;

    public IOHandler(Configuration hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }

    public void delete(String file) {
        if (file.startsWith("hdfs:/")) {
            deleteHdfs(file, false);
        } else {
            deleteVfs(file);
        }
    }

    public void deleteFolder(String folder) {
        if (folder.startsWith("hdfs:/")) {
            deleteHdfs(folder, true);
        } else {
            deleteFolderVfs(folder);
        }
    }

    private void deleteVfs(String file) {
        try {
            FileSystemManager fsManager = VFS.getManager();
            FileObject fileObject = fsManager.resolveFile(file);
            if(!fileObject.exists()){
                return;
            }
            fileObject.delete();
        } catch (IOException e) {
            throw new RuntimeException("Unexpected error trying to delete file.", e);
        }
    }

    private void deleteFolderVfs(String folder) {
        try {
            FileSystemManager fsManager = VFS.getManager();
            FileObject fileObject = fsManager.resolveFile(folder);
            if(!fileObject.exists()){
               return;
            }

            Stream.of(fileObject.getChildren()).forEach(file -> {
                try {
                    file.delete();
                } catch (FileSystemException e) {
                    throw new RuntimeException("Unable to delete file: " + file.toString(), e);
                }
            });
            fileObject.delete();
        } catch (IOException e) {
            throw new RuntimeException("Unexpected error trying to delete directory.", e);
        }
    }

    private void deleteHdfs(String folder, boolean recursive) {
        try {
            FileSystem fs = FileSystem.get(hadoopConfig);
            fs.delete(new Path(folder), recursive);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected error trying to delete directory.", e);
        }
    }

}
