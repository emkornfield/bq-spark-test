import com.google.cloud.bigquery.connector.common.BigQueryConfig;
import com.google.cloud.bigquery.connector.common.UserAgentProvider;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.v2.BigQueryDataSourceReader;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class BigQueryStorageDsv2Runner {

    static Options getParseOptions() {
        Options parseOptions = new Options();
        parseOptions.addOption(
                Option.builder("p")
                        .longOpt("parent")
                        .desc("The ID of the parent project for the read session")
                        .required()
                        .hasArg()
                        .type(String.class)
                        .build());
        parseOptions.addOption(
                Option.builder("t")
                        .longOpt("table")
                        .desc("The fully-qualified ID of the table to read from")
                        .required()
                        .hasArg()
                        .type(String.class)
                        .build());
        parseOptions.addOption(
                Option.builder("s")
                        .longOpt("streams")
                        .desc("The number of streams to request during session creation")
                        .hasArg()
                        .type(Integer.class)
                        .build());

        return parseOptions;
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(getParseOptions(), args);


        String table = commandLine.getOptionValue("table");
        String parentProject = commandLine.getOptionValue("parent");

        String streams = commandLine.getOptionValue("streams", "1");

        System.out.println("Table: " + table);
        System.out.println("Parent: " + parentProject);
        System.out.println("Data format: " + commandLine.getOptionValue("format"));
        SparkBigQueryConfig config = SparkBigQueryConfig.from(
                ImmutableMap.of("parallelism", streams,
                        "table", table),

                ImmutableMap.of("parentProject", parentProject),
                new Configuration(),
                /*defaultParallelism=*/Integer.parseInt(streams),
                new SQLConf(), /*sparkVersion=*/"2.4.6", /*schema=*/Optional.empty());

        Injector injector = Guice.createInjector(new com.google.cloud.bigquery.connector.common.BigQueryClientModule(),
                new com.google.cloud.spark.bigquery.v2.BigQueryDataSourceReaderModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(BigQueryConfig.class).toInstance(config);
                        bind(UserAgentProvider.class).toInstance(() -> "sparkClientProfiler");
                    }
                });
        Stopwatch stopwatch = Stopwatch.createStarted();
        BigQueryDataSourceReader reader = injector.getInstance(BigQueryDataSourceReader.class);
        List<InputPartition<ColumnarBatch>> partitions = reader.planBatchInputPartitions();
        stopwatch.stop();
        long elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        double displaySeconds = elapsedMillis / 1000.0;
        System.out.println("Read session creation took " + displaySeconds + " seconds");

        System.out.printf("Creating a reader thread for %d streams ", partitions.size());
        List<ReaderThread> readerThreads = new ArrayList<>();
        int streamNumber = 0;
        for (InputPartition<ColumnarBatch> partition : partitions) {
            InputPartitionReader<ColumnarBatch> partitionReader = partition.createPartitionReader();
            readerThreads.add(new ReaderThread(streamNumber, partitionReader));
            streamNumber++;
        }

        for (ReaderThread readerThread : readerThreads) {
            readerThread.start();
        }
        System.out.println("All reader threads started.");

        for (ReaderThread readerThread : readerThreads) {
            readerThread.join();
        }
        System.out.println("All reader threads finished; exiting");
    }

    static class ReaderThread extends Thread {
        private final int streamNumber;
        private final InputPartitionReader<ColumnarBatch> partitionReader;

        long numResponses = 0;
        long numRows = 0;
        long numTotalBytes = 0;
        long lastReportTimeMicros = 0;

        public ReaderThread(int streamNumber, InputPartitionReader<ColumnarBatch> partitionReader) {
            this.streamNumber = streamNumber;
            this.partitionReader = partitionReader;
        }

        public void run() {
            try {
                readRows();
            } catch (Exception e) {
                System.err.println("Caught exception while calling ReadRows: " + e);
            }
        }

        private void readRows() throws Exception {
            try {
                Stopwatch stopwatch = Stopwatch.createStarted();
                while (partitionReader.next()) {
                    ColumnarBatch batch = partitionReader.get();
                    numResponses++;
                    numRows += batch.numRows();
                    printPeriodicUpdate(stopwatch.elapsed(TimeUnit.MICROSECONDS));

                }

                stopwatch.stop();
                printPeriodicUpdate(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                System.out.println("Finished reading from stream: " + streamNumber);
            } finally {
                partitionReader.close();
            }
        }

        private void printPeriodicUpdate(long elapsedMicros) {
            if (elapsedMicros - lastReportTimeMicros < TimeUnit.SECONDS.toMicros(10)) {
                return;
            }

            System.out.printf(
                    "%d: Received %d responses (%d rows) from stream in 10s",
                    streamNumber, numResponses, numRows);

            numResponses = 0;
            numRows = 0;
            numTotalBytes = 0;
            lastReportTimeMicros = elapsedMicros;
        }
    }
}
