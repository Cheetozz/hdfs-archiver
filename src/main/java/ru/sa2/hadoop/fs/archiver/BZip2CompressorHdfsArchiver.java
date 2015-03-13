package ru.sa2.hadoop.fs.archiver;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by alvyasmirnov on 04.03.2015.
 */
public class BZip2CompressorHdfsArchiver implements HdfsArchiver {

    private Logger LOG = Logger.getLogger(this.getClass());
    private static String suffix = ".bz2";
    private Configuration conf;
    private FileSystem hdfs;
    private int BLOCKSIZE;


    public BZip2CompressorHdfsArchiver(Configuration configuration) throws IOException {
        conf = configuration;
        hdfs = FileSystem.get(conf);
        BLOCKSIZE = 25 * 1024;
    }


    @Override
    public void fileToArchive(Path sourceFile, Path targetDir) throws IOException {
        //TODO реализовать
    }

    @Override
    public void mergeToArchive(FileStatus[] mergingFiles, Path targetDir, String archiveName) throws IOException {
        Path out = new Path(targetDir + File.separator + archiveName + suffix);
        checkOutPath(out);

        FSDataOutputStream outputStream = hdfs.create(out);
        BZip2CompressorOutputStream bzip2Out = new BZip2CompressorOutputStream(outputStream);

        byte[] buffer = new byte[BLOCKSIZE];
        int bytesRead;

        for (FileStatus file : mergingFiles) {
            FSDataInputStream inputStream = hdfs.open(file.getPath());
            while ((bytesRead = inputStream.read(buffer, 0, BLOCKSIZE)) != -1) {
                bzip2Out.write(buffer, 0, bytesRead);
            }
            bzip2Out.write("\n".getBytes());
            inputStream.close();
        }

        bzip2Out.flush();
        bzip2Out.close();
        outputStream.close();
    }

    @Override
    public void streamToArchive(InputStream inputStr, Path targetDir, String archiveName) throws IOException {
        //TODO реализовать
    }

    private void checkOutPath(Path out) throws IOException {
        if (hdfs.exists(out)) {
            LOG.info("Path " + out + " already exist. Remove.");
            deletePath(out);
        }
    }

    private void deletePath(Path out) throws IOException {
        hdfs.delete(out, true);
    }
}
