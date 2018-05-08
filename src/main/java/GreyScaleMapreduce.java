import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.awt.image.BufferedImage;
import java.io.IOException;


/**
 * Created by AMakoviczki on 2018. 05. 08..
 */
public class GreyScaleMapreduce {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"GreyScale Mapreduce");
        job.setJarByClass(GreyScaleMapreduce.class);
        job.setMapperClass(ImgGreyMapper.class);
        //job.setCombinerClass();
        //job.setReducerClass();

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class ImgGreyMapper extends Mapper<Text,BytesWritable,Text,BytesWritable>{

        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            /*SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(outputPath));

            Text fileKey = new Text();
            BytesWritable fileValue = new BytesWritable();

            while (reader.next(fileKey, fileValue)) {
                System.out.println(key.toString() + " " + value.getLength());
            }

            IOUtils.closeStream(reader);*/

            System.out.println(key);

            context.write(key,value);
        }
    }
}