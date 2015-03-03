package archiver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by AlVyaSmirnov on 03.03.2015.
 */
public interface HdfsArchiver {

    //    public void getInstance(Configuration conf);
    public void fileToArchive(Path sourceFile, Path targetDir)
            throws IOException;

    public void mergeToArchive(FileStatus[] mergingFiles, Path targetDir, String archiveName)
            throws IOException;

    public void streamToArchive(InputStream inputStr, Path targetDir, String archiveName)
            throws IOException;

}
