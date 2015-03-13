package ru.sa2.hadoop.fs.archiver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Created by alvyasmirnov on 18.02.2015.
 */
public class ZipHdfsArchiver implements HdfsArchiver{

    private Logger LOG = Logger.getLogger(this.getClass());
    private static String suffix = ".zip";
    private Configuration conf;
    private FileSystem hdfs;
    private int BLOCKSIZE;


    public ZipHdfsArchiver(Configuration configuration) throws IOException {
        conf = configuration;
        hdfs = FileSystem.get(conf);
        BLOCKSIZE = 25 * 1024;
    }

    /**
     * Создает zip-архив файла в hdfs.
     *
     * @param sourceFile - архивируемый файл
     * @param targetDir  - папка с архивом
     * @throws java.io.IOException
     */
    @Override
    public void fileToArchive(Path sourceFile, Path targetDir) throws IOException {

        if (!hdfs.exists(sourceFile)) throw new FileNotFoundException("File:\t" + sourceFile);
        if (hdfs.isDirectory(sourceFile))
            dirToArchive(sourceFile, targetDir);
//            throw new IllegalArgumentException("Directories not supported, use .zipDir() method");
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
        ZipOutputStream zOut = new ZipOutputStream(outputStream);
        zOut.putNextEntry(new ZipEntry(archiveName));

        byte[] buffer = new byte[BLOCKSIZE];
        int bytesRead;

        for (FileStatus status : mergingFiles) {

            Path file = status.getPath();

            LOG.info("Merging file:\t" + file);

            if (!hdfs.exists(file)) {
                zOut.closeEntry();
                zOut.flush();
                zOut.close();
                outputStream.close();
                throw new FileNotFoundException("File:\t" + file);
            }

            FSDataInputStream inputStream = hdfs.open(file);
            while ((bytesRead = inputStream.read(buffer, 0, BLOCKSIZE)) != -1) {
                zOut.write(buffer, 0, bytesRead);
            }
            //TODO если у файла нет в конце пустой строки - последняя  и первая строчки смежных файлов склеиваются
//            zOut.write("\n".getBytes());
            inputStream.close();
        }

        zOut.closeEntry();
        zOut.flush();
        zOut.close();
        outputStream.close();

        LOG.info("SUCCESS!");
        LOG.info("======================");
    }

    @Override
    public void streamToArchive(InputStream inputStream, Path targetDir, String archiveName) throws IOException {

        Path out = getOutPath(targetDir, archiveName);

        FSDataOutputStream outputStream = hdfs.create(out);
        ZipOutputStream zout = new ZipOutputStream(outputStream);
        zout.putNextEntry(new ZipEntry(archiveName));

        byte[] buffer = new byte[BLOCKSIZE];

        int bytesRead;
        while ((bytesRead = inputStream.read(buffer, 0, BLOCKSIZE)) != -1) {
            zout.write(buffer, 0, bytesRead);
        }

        zout.closeEntry();
        zout.flush();
        zout.close();
        inputStream.close();
        outputStream.close();
        LOG.info("File archive:\t" + out);
        LOG.info("SUCCESS!");
        LOG.info("======================");
    }


    public void dirToArchive(Path sourceDir, Path targetDir) throws IOException {
        LOG.info("======================");
        LOG.info("START ARCHIVING");

        if (!hdfs.exists(sourceDir)) throw new FileNotFoundException("File:\t" + sourceDir);
        if (!hdfs.isDirectory(sourceDir))
            fileToArchive(sourceDir, targetDir);

        Path out = getOutPath(targetDir, sourceDir.getName());
        LOG.info("Archive file:\t" + out);

        FSDataOutputStream outputStream = hdfs.create(out);
        ZipOutputStream zout = new ZipOutputStream(outputStream);

        RemoteIterator<LocatedFileStatus> files = hdfs.listFiles(sourceDir, true); //true - recursive
        while (files.hasNext()) {
            Path file = files.next().getPath();
//            System.out.println(file);
//            System.out.println(file.toString().replaceAll("^.*" + sourceDir.toString() + "/", ""));

            FSDataInputStream inputStream = hdfs.open(file);
            zout.putNextEntry(new ZipEntry(file
                    .toString()
                    .replaceAll("^.*" + sourceDir.getParent().toString() + "/", "")));

            byte[] buffer = new byte[BLOCKSIZE];

            int bytesRead;
            while ((bytesRead = inputStream.read(buffer, 0, BLOCKSIZE)) != -1) {
                zout.write(buffer, 0, bytesRead);
            }
            zout.closeEntry();
            zout.flush();
            inputStream.close();
            LOG.info("Put file in archive:\t" + file);
        }
        zout.close();
        outputStream.close();
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