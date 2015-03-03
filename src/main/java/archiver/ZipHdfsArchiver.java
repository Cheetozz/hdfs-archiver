package archiver;

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
     * @throws IOException
     */
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
        ZipOutputStream zOut = new ZipOutputStream(outputStream);
        zOut.putNextEntry(new ZipEntry(archiveName));

        byte[] buffer = new byte[BLOCKSIZE];
        int bytesRead;

        for (FileStatus file : mergingFiles) {
            FSDataInputStream inputStream = hdfs.open(file.getPath());
            while ((bytesRead = inputStream.read(buffer, 0, BLOCKSIZE)) != -1) {
                zOut.write(buffer, 0, bytesRead);
            }
            zOut.write("\n".getBytes());
            inputStream.close();
        }
        zOut.closeEntry();
        zOut.flush();
        zOut.close();
        outputStream.close();

    }

    @Override
    public void streamToArchive(InputStream inputStream, Path targetDir, String archiveName) throws IOException {

        Path out = new Path(targetDir + File.separator + archiveName + suffix);

        checkOutPath(out);

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
    }

    public void zipDir(Path sourceDir, Path targetDir) throws IOException {

        //ToDo write directory archiver
        RemoteIterator<LocatedFileStatus> files = hdfs.listFiles(sourceDir, true); //true - recursive
        while (files.hasNext()) {
            System.out.println(files.next().getPath());
        }

    }

    private void checkOutPath(Path out) throws IOException {
        if (hdfs.exists(out)) {
            hdfs.delete(out, true);
            LOG.info("File archive already exist. Remove.");
        }
    }

}
