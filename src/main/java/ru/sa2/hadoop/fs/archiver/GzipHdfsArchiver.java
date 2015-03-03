package ru.sa2.hadoop.fs.archiver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by AlVyaSmirnov on 03.03.2015.
 */
public class GzipHdfsArchiver implements HdfsArchiver {

    private Logger LOG = Logger.getLogger(this.getClass());
    private static String suffix = ".gz";
    private Configuration conf;
    private FileSystem hdfs;
    private int BLOCKSIZE;


    public GzipHdfsArchiver(Configuration configuration) throws IOException {
        conf = configuration;
        hdfs = FileSystem.get(conf);
        BLOCKSIZE = 25 * 1024;
    }

    @Override
    public void fileToArchive(Path sourceFile, Path targetDir) throws IOException {

        if (!hdfs.exists(sourceFile)) throw new FileNotFoundException();
        if (hdfs.isDirectory(sourceFile))
            throw new IllegalArgumentException("Directories not supported, use Zipper.zipDirectory()");

        streamToArchive(hdfs.open(sourceFile), targetDir, sourceFile.getName());

    }

    @Override
    public void mergeToArchive(FileStatus[] mergingFiles, Path targetDir, String archiveName) throws IOException {

        Path out = new Path(targetDir + File.separator + archiveName + suffix);
        checkOutPath(out);

        FSDataOutputStream outputStream = hdfs.create(out);
        GZIPOutputStream gzOut = new GZIPOutputStream(outputStream);

        byte[] buffer = new byte[BLOCKSIZE];
        int bytesRead;

        for (FileStatus file : mergingFiles) {
            FSDataInputStream inputStream = hdfs.open(file.getPath());
            while ((bytesRead = inputStream.read(buffer, 0, BLOCKSIZE)) != -1) {
                gzOut.write(buffer, 0, bytesRead);
            }
            gzOut.write("\n".getBytes());
            inputStream.close();
        }

        gzOut.flush();
        gzOut.close();
        outputStream.close();
    }

    @Override
    public void streamToArchive(InputStream inputStream, Path targetDir, String archiveName) throws IOException {

        Path out = new Path(targetDir + File.separator + archiveName + suffix);
        checkOutPath(out);

        FSDataOutputStream outputStream = hdfs.create(out);
        GZIPOutputStream gzOut = new GZIPOutputStream(outputStream);

        byte[] buffer = new byte[BLOCKSIZE];
        int bytesRead;

//        FSDataInputStream inputStream = (FSDataInputStream) inputStr;
        while ((bytesRead = inputStream.read(buffer, 0, BLOCKSIZE)) != -1) {
            gzOut.write(buffer, 0, bytesRead);
        }

        gzOut.flush();
        gzOut.close();
        inputStream.close();
        outputStream.close();

    }

    private void checkOutPath(Path out) throws IOException {
        if (hdfs.exists(out)) {
            hdfs.delete(out, true);
            LOG.info("File archive already exist. Remove.");
        }
    }
}
