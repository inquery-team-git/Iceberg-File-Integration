import java.io.IOException;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.TableProperties;

import com.google.common.collect.Lists;

import software.amazon.awssdk.services.glue.GlueClient;
public class newFileAppender {
    public static void ensureNameMappingPresent(Table table) {
        if (table.properties().get(TableProperties.DEFAULT_NAME_MAPPING) == null) {
            // Forces Name based resolution instead of position based resolution
            NameMapping mapping = MappingUtil.create(table.schema());
            String mappingJson = NameMappingParser.toJson(mapping);
            table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();
        }
    }
    public static void processParquetFile(String filePath,String databaseName, String tableName) throws IOException {
        Configuration conf = new Configuration();
        // Use DefaultCredentialsProvider for AWS authentication
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        // Ensure fs.s3a.impl is set to use S3AFileSystem
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);
        long fileSize = fs.getFileStatus(path).getLen();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
        ParquetMetadata metadata = ParquetFileReader.readFooter(inputFile, ParquetMetadataConverter.NO_FILTER);
        long rowCount = metadata.getBlocks().stream().mapToLong(block -> block.getRowCount()).sum();
	    fs.close();
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
        Schema icebergSchema = SparkSchemaUtil.convert(sparkSchema);

        // Print the Iceberg schema
        String warehouseLocation = AppConfig.WAREHOUSE_LOCATION;

        //String awsRegion = "us-east-2";
        Map < String, String > properties = new HashMap < > ();
        properties.put("warehouse", warehouseLocation);
        properties.put("client", "glue");

        // Assuming use of GlueClient directly for demonstration
        try (GlueClient glueClient = GlueClient.builder().build()) {
            GlueCatalog catalog = new GlueCatalog();
            catalog.setConf(new org.apache.hadoop.conf.Configuration());

            // If GlueCatalog required direct GlueClient, it should be managed here
            catalog.initialize("glue_catalog", properties);
            PartitionSpec partitionspec = PartitionSpec.unpartitioned();
            TableIdentifier identifier = TableIdentifier.of(databaseName, tableName);

            // Define the partition spec, if needed
            PartitionSpec spec = PartitionSpec.unpartitioned();

            // Create the table
            Table table = catalog.loadTable(identifier);
            if (table.spec().isUnpartitioned()) {
                ensureNameMappingPresent(table);
                DataFiles.Builder builder = DataFiles.builder(table.spec())
                    .withRecordCount(rowCount)
                    .withInputFile(table.io().newInputFile(filePath));

                table.newAppend()
                    .appendFile(builder.build())
                    .commit();
                System.out.println("File appended successfully to table " + tableName);
            } else {
                System.out.println("Nothing done since table is partitioned");
            }
        }
    }
    public static void main(String[] args) throws IOException {

	if(args.length < 3) {
            System.out.println("Usage: DirectorySchemaReader <directoryPath> <tableName>");
            return;
        }

        // Example Parquet file path in S3
        String filePath = args[0];
        String tableName = args[2];
	    String databaseName = args[1];

        processParquetFile(filePath,databaseName, tableName);
    }
}
