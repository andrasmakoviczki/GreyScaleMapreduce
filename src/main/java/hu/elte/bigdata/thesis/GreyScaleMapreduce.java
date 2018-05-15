package hu.elte.bigdata.thesis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.opencv.core.Core;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;


/**
 * Created by AMakoviczki on 2018. 05. 08..
 */
public class GreyScaleMapreduce {
    private final Logger logger = LoggerFactory.getLogger(hu.elte.bigdata.thesis.GreyScaleMapreduce.class);

    public static void main(String[] args) throws Exception {
        //Load the native library for OpenCV
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);

        //Set configuration
        Configuration conf = new Configuration();

        //Set the job
        Job job = Job.getInstance(conf, "GreyScale Mapreduce");
        job.setJarByClass(GreyScaleMapreduce.class);
        job.setMapperClass(ImgGreyMapper.class);
        job.setNumReduceTasks(1);

        //Set input
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        //Set output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Launch job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class ImgGreyMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
        private final Logger logger = LoggerFactory.getLogger(ImgGreyMapper.class);

        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

            // Create input stream
            ImageInputStream inputIO = ImageIO.createImageInputStream(new ByteArrayInputStream(value.getBytes()));
            BufferedImage bImageFromConvert = null;
            byte[] byteImage = null;

            try {
                // Get the reader
                Iterator<ImageReader> readers = ImageIO.getImageReaders(inputIO);

                if (readers.hasNext()) {
                    ImageReader reader = readers.next();
                    try {
                        reader.setInput(inputIO);
                        ImageReadParam param = reader.getDefaultReadParam();
                        bImageFromConvert = reader.read(0, param);
                    } finally {
                        // Dispose reader
                        reader.dispose();
                    }
                }
            } finally {
                // Close stream
                inputIO.close();
            }

            if (bImageFromConvert != null) {
                try {

                    //Get the image file
                    byte[] data = ((DataBufferByte) bImageFromConvert.getRaster().getDataBuffer()).getData();
                    Mat mat = new Mat(bImageFromConvert.getHeight(), bImageFromConvert.getWidth(), CvType.CV_8UC3);
                    mat.put(0, 0, data);

                    Mat mat1 = new Mat(bImageFromConvert.getHeight(), bImageFromConvert.getWidth(), CvType.CV_8UC1);
                    Imgproc.cvtColor(mat, mat1, Imgproc.COLOR_RGB2GRAY);

                    //Init new greyscale image
                    byte[] data1 = new byte[mat1.rows() * mat1.cols() * (int) (mat1.elemSize())];
                    mat1.get(0, 0, data1);

                    //Copy the image into greyscale image
                    BufferedImage image1 = new BufferedImage(mat1.cols(), mat1.rows(), BufferedImage.TYPE_BYTE_GRAY);
                    image1.getRaster().setDataElements(0, 0, mat1.cols(), mat1.rows(), data1);

                    //Convert image back to byte array
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ImageIO.write(image1, "jpg", baos);
                    byteImage = baos.toByteArray();

                } catch (Exception e) {
                    System.out.println("Exception");
                }
            }

            context.write(key, new BytesWritable(byteImage));

        }
    }
}