package cc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CharCount {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("분석할 폴더 및 분석결과가 저장될 폴더를 입력해야합니다.");
            System.exit(-1);
        }

        Job job = Job.getInstance();

        job.setJarByClass(CharCount.class);

        job.setJobName("char count");

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CharCountMapper.class);

        job.setReducerClass(CharCountReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}