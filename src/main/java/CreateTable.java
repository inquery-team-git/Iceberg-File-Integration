import org.apache.iceberg.Schema;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.DataFiles;
import static org.apache.iceberg.mapping.NameMappingParser.toJson;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.PartitionSpec;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.hadoop.fs.Path;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import org.apache.iceberg.types.Types;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.iceberg.parquet.ParquetUtil;
import java.util.stream.Stream;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import com.google.errorprone.annotations.Immutable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.types.StructType;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.glue.GlueClient;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.parquet.schema.MessageType;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.parquet.hadoop.ParquetFileReader;
import com.google.common.collect.ImmutableMap;


public class CreateTable {
    public static void main(String[] args) {
        if(args.length < 3) {
            System.out.println("Usage CreateTable <filePath> <databaseName><tableName>");
            System.out.println(args[0]);
	        return;
        }
	    List<String> filePaths = new ArrayList<>();
        String filePath = args[0];
        filePaths.add(filePath);
        if (filePaths.isEmpty()) {
            return;
        }
        String warehouseLocation = AppConfig.WAREHOUSE_LOCATION;
        String databaseName = args[1];
        String tableName = args[2];        

        String storageFormat = "PARQUET";

        Configuration conf = getAWSConfig();
        Path path = new Path(filePaths.get(0));
        long fileSize;
        ParquetMetadata metadata;

        try {
            FileSystem fs = path.getFileSystem(conf);
            fileSize = fs.getFileStatus(path).getLen();

            HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
            metadata = ParquetFileReader.readFooter(inputFile, ParquetMetadataConverter.NO_FILTER);
        } catch (Exception e) {
            return;
        }

        long rowCount = metadata.getBlocks().stream().mapToLong(block -> block.getRowCount()).sum();
        System.out.println("Row count: " + rowCount);
 
        Schema schema = parquetToIceSchema(path, conf);
        System.out.println("new Schema " + schema); 

        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("warehouse", warehouseLocation);
        catalogProperties.put("client", "glue");
        PartitionSpec partitionspec = PartitionSpec.unpartitioned();

        try (GlueClient glueClient = GlueClient.builder().build()) {
            ImmutableList.Builder<DataFile> dataFilesBuilder = ImmutableList.builder();
            if(partitionspec.isUnpartitioned()) {
                // Handle this correctly later
                // Metrics metrics = loadMetrics(schema);
                DataFile dataFile = buildDataFile(filePath, partitionspec, storageFormat, fileSize, rowCount);
                dataFilesBuilder.add(dataFile);
            }
            else {
                // Metrics metrics = loadMetrics(schema);
                DataFile dataFile = buildDataFile(filePath, partitionspec, storageFormat, fileSize, rowCount);
                dataFilesBuilder.add(dataFile);
            }
            List<DataFile> dataFiles = dataFilesBuilder.build();

            GlueCatalog catalog = new GlueCatalog();
            catalog.setConf(new org.apache.hadoop.conf.Configuration());

            // If GlueCatalog required direct GlueClient, it should be managed here
            catalog.initialize("glue_catalog", catalogProperties);

            TableIdentifier identifier = TableIdentifier.of(databaseName, tableName);
          
            Transaction transaction = catalog.buildTable(identifier, schema)
                    .withPartitionSpec(partitionspec)
                    .withLocation(null)
                    .createTransaction();

            Table table = transaction.table();
            newFileAppender.ensureNameMappingPresent(table);
            AppendFiles append = table.newAppend();
            dataFiles.forEach(append::appendFile);
            append.commit();
            transaction.commitTransaction();
            System.out.println("Successfully Committed Transactions");
        }
    }

    private static Configuration getAWSConfig() {
        Configuration conf = new Configuration();

        // Use DefaultCredentialsProvider for AWS authentication
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        // Ensure fs.s3a.impl is set to use S3AFileSystem
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        return conf;
    }

    private static Schema parquetToIceSchema(Path path, Configuration conf) {
        try {
            HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
            ParquetMetadata metadata = ParquetFileReader.readFooter(inputFile, ParquetMetadataConverter.NO_FILTER);
            MessageType parquetSchema = metadata.getFileMetaData().getSchema();

            // Initialize the converter. Adjust the constructor parameters based on your requirements.
            boolean assumeBinaryIsString = true; // or false, depending on your data
            boolean assumeInt96IsTimestamp = true; // or false
            boolean caseSensitive = true; // or false
            boolean inferTimestampNTZ = false; // true if you want to infer TIMESTAMP_NTZ for int96 fields
            boolean nanosAsLong = false; // true if you are working with nanoseconds precision and want them as long
                
            ParquetToSparkSchemaConverter converter = new ParquetToSparkSchemaConverter(
                assumeBinaryIsString, 
                assumeInt96IsTimestamp, 
                caseSensitive, 
                inferTimestampNTZ, 
                nanosAsLong
            );

            // Convert the MessageType (Parquet schema) to StructType (Spark schema)
            StructType sparkSchema = converter.convert(parquetSchema);

            // Print the Spark schema
            Schema icebergSchema = SparkSchemaUtil.convert(sparkSchema);
            return icebergSchema;
        } catch (Exception e) {
            e.printStackTrace(); 
        throw new RuntimeException("Conversion from Parquet to Iceberg schema failed", e);
        }
    }

    // private static Metrics loadMetrics(Schema schema) {
    //     // For each file type, read the footer and return 
    //     ParquetMetadata metadata = readFileFooter(fileFooter); // Needs to be implemented
    //     return ParquetUtil.footerMetrics(metadata, Stream.empty(), MetricsConfig.getDefault(), MappingUtil.create(schema));
    // }

    private static DataFile buildDataFile(String filePath, PartitionSpec spec, String format, long fileSize, long recordCount) // , Metrics metrics)
        {
            return DataFiles.builder(spec)
                    .withPath(filePath)
                    .withFormat(format)
                    .withFileSizeInBytes(fileSize)
                    .withRecordCount(recordCount)
                    // .withMetrics(metrics)
                    // .withPartition(partition)
                    .build();
        }
}
