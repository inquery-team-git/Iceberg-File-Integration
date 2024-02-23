import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

public class DirectorySchemaReader {
    public static void main(String[] args) throws IOException {
        if(args.length < 3) {
            return;
        }

	    Configuration conf = new Configuration();

        // Use DefaultCredentialsProvider for AWS authentication
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        // Ensure fs.s3a.impl is set to use S3AFileSystem
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // Directory path in S3 containing Parquet files
        String directoryPath = args[0];
        Path dirPath = new Path(directoryPath);
        FileSystem fs = dirPath.getFileSystem(conf);
        String tableName = args[2];
        String dataBase = args[1];

        // List all files in the directory
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(dirPath, false);
        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            String filePath = fileStatus.getPath().toString();
            if (filePath.endsWith(".parquet")) { // Check if the file is a Parquet file
                System.out.println("Reading Parquet file: " + filePath);
	        newFileAppender.processParquetFile(filePath,dataBase, tableName);
            }
        }
    }
}

