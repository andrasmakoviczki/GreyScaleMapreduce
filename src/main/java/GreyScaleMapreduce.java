import core.writables.BufferedImageWritable;
import opencv.MatImageOutputFormat;
import opencv.MatImageWritable;
import opencv.OpenCVMapper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import core.formats.BufferedImage.BufferedImageOutputFormat;


/**
 * Created by AMakoviczki on 2018. 05. 08..
 */
public class GreyScaleMapreduce {
    private final Logger logger = LoggerFactory.getLogger(GreyScaleMapreduce.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "GreyScale Mapreduce");
        job.setJarByClass(GreyScaleMapreduce.class);
        job.setMapperClass(ImgGreyMapper.class);
        //job.setCombinerClass();
        //job.setReducerClass();
        job.setNumReduceTasks(1);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MatImageOutputFormat.class);
        //FileOutputFormat.setCompressOutput(job, true);
        //FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class ImgGreyMapper extends OpenCVMapper<Text, BytesWritable, Text, BufferedImageWritable> {
        private final Logger logger = LoggerFactory.getLogger(ImgGreyMapper.class);

        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

            // Create input stream
            ImageInputStream inputIO = ImageIO.createImageInputStream(new ByteArrayInputStream(value.getBytes()));
            BufferedImage image = null;

            try {
                // Get the reader
                Iterator<ImageReader> readers = ImageIO.getImageReaders(inputIO);

                if (readers.hasNext()) {
                    ImageReader reader = readers.next();
                    try {
                        reader.setInput(inputIO);
                        ImageReadParam param = reader.getDefaultReadParam();
                        image = reader.read(0, param);
                        int numThumbs = reader.getNumThumbnails(0);
                    } finally {
                        // Dispose reader in finally block to avoid memory leaks
                        reader.dispose();
                    }
                }
            } finally {
                // Close stream in finally block to avoid resource leaks
                inputIO.close();
            }

            if (image != null) {
                try {
                    byte[] data = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
                    Mat mat = new Mat(image.getHeight(), image.getWidth(), CvType.CV_8UC3);
                    mat.put(0, 0, data);

                    Mat mat1 = new Mat(image.getHeight(), image.getWidth(), CvType.CV_8UC1);
                    Imgproc.cvtColor(mat, mat1, Imgproc.COLOR_RGB2GRAY);

                    byte[] data1 = new byte[mat1.rows() * mat1.cols() * (int) (mat1.elemSize())];
                    mat1.get(0, 0, data1);
                    BufferedImage image1 = new BufferedImage(mat1.cols(), mat1.rows(), BufferedImage.TYPE_BYTE_GRAY);
                    image1.getRaster().setDataElements(0, 0, mat1.cols(), mat1.rows(), data1);

                    BufferedImageWritable image2 = new BufferedImageWritable(image1);
                    context.write(key, image2);
                } catch (UnsupportedOperationException e) {
                    System.out.println("UnsupportedOperationException");
                }
            }


        }
    }
}