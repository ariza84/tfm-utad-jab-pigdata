package com.tfm.utad.pigdata;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PigData {

    private static final String HDFS_LOCALHOST_LOCALDOMAIN = "hdfs://172.16.134.128/";
    private static final String FS_DEFAULT_FS = "fs.defaultFS";
    private static final String INPUT_DIRECTORY = "/home/jab/camus/pigdata";

    private final static Logger LOG = LoggerFactory.getLogger(PigData.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration(true);
        conf.set(FS_DEFAULT_FS, HDFS_LOCALHOST_LOCALDOMAIN);
        FileSystem fs = FileSystem.get(conf);
        Path filesPath = new Path(INPUT_DIRECTORY + "/*/part-r*");
        FileStatus[] files = fs.globStatus(filesPath);
        for (FileStatus fStatus : files) {
            LOG.info("Path name:" + fStatus.getPath());
            int output = exec(fStatus.getPath());
            if (output == 0) {
                LOG.info("Removing directory in path:" + fStatus.getPath().getParent());
                fs.delete(fStatus.getPath().getParent(), true);
            } else {
                LOG.error("Pig FAILED exec file:" + fStatus.getPath() + ".Please, contact the system administrator.");
            }
        }
    }

    private static int exec(Path input) {
        int result;
        String command = "pig -param INPUT=" + input.toString() + " -f tfm-utad.pig";
        Process process;
        try {
            process = Runtime.getRuntime().exec(command);
            long start_time = System.currentTimeMillis();
            result = process.waitFor();
            long end_time = System.currentTimeMillis();
            long difference = end_time - start_time;
            LOG.info("MapReduce script execution time: "
                    + String.format("%d min %d sec",
                            TimeUnit.MILLISECONDS.toMinutes(difference),
                            TimeUnit.MILLISECONDS.toSeconds(difference) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(difference))
                    ));
        } catch (IOException | InterruptedException ex) {
            result = 1;
        }
        return result;
    }
}