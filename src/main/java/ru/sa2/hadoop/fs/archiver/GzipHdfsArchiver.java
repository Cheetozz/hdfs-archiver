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

        if (!hdfs.exists(sourceFile)) throw new FileNotFoundException("File:\t" + sourceFile);
        if (hdfs.isDirectory(sourceFile))
            throw new IllegalArgumentException("Directories not supported");
        LOG.info("======================");
        LOG.info("START ARCHIVING");
        LOG.info("File source:\t" + sourceFile);

        streamToArchive(hdfs.open(sourceFile), targetDir, sourceFile.getName());
    }

    @Override
    public void mergeToArchive(FileStatus[] mergingFiles, Path targetDir, String archiveName) throws IOException {
        LOG.info("======================");
        LOG.info("START ARCHIVING");

        Path out = getOutPath(targetDir, archiveName);

        LOG.info("Archive file:\t" + out);

        FSDataOutputStream outputStream = hdfs.create(out);
        GZIPOutputStream gzOut = new GZIPOutputStream(outputStream);

        byte[] buffer = new byte[BLOCKSIZE];
        int bytesRead;

        for (FileStatus status : mergingFiles) {

            Path file = status.getPath();

            LOG.info("Merging file:\t" + file);

            if (hdfs.exists(file)){
                gzOut.flush();
                gzOut.close();
                outputStream.close();
                throw new FileNotFoundException("File:\t" + file);
            }

            FSDataInputStream inputStream = hdfs.open(file);
            while ((bytesRead = inputStream.read(buffer, 0, BLOCKSIZE)) != -1) {
                gzOut.write(buffer, 0, bytesRead);
            }
            //TODO если у файла нет в конце пустой строки - последняя  и первая строчки смежных файлов склеиваются
//            gzOut.write("\n".getBytes());
            inputStream.close();
        }

        gzOut.flush();
        gzOut.close();
        outputStream.close();

        LOG.info("SUCCESS!");
        LOG.info("======================");
    }

    @Override
    public void streamToArchive(InputStream inputStream, Path targetDir, String archiveName) throws IOException {

        Path out = getOutPath(targetDir, archiveName);

        FSDataOutputStream outputStream = hdfs.create(out);
        GZIPOutputStream gzOut = new GZIPOutputStream(outputStream);

        byte[] buffer = new byte[BLOCKSIZE];
        int bytesRead;

        while ((bytesRead = inputStream.read(buffer, 0, BLOCKSIZE)) != -1) {
            gzOut.write(buffer, 0, bytesRead);
        }

        gzOut.flush();
        gzOut.close();
        inputStream.close();
        outputStream.close();

        LOG.info("File archive:\t" + out);
        LOG.info("SUCCESS!");
        LOG.info("======================");
    }

    private Path getOutPath(Path targetDir, String archiveName) throws IOException{
        Path outPath = new Path(targetDir + File.separator + archiveName + suffix);
        checkOutPath(outPath);
        return outPath;
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
