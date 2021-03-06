package com.gear11.warc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.commoncrawl.warc.WARCFileInputFormat;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

/**
 * A Map-Reduce job for scanning Common Crawl data for Geo RSS feeds.
 */
public class GeoRSSCounter extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(GeoRSSCounter.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
        FEEDS_IN,
        GEO_RSS_IN,
		EXCEPTIONS
	}

    /**
     * Scans the input archives for documents that are geo-enabled RSS feeds.  For each, outputs
     * URL as key, and an array of longs as the value:
     * 0: Updated time in seconds since epoch
     * 1: Number of geotagged items
     * 2: Number of distinct locations
     */
	protected static class GeoRSSCounterMapper extends Mapper<Text, ArchiveReader, Text, ArrayPrimitiveWritable> {
		private final Text outKey = new Text();
        private final long[] vals = new long[3];
        private final ArrayPrimitiveWritable result = new ArrayPrimitiveWritable(vals);

		@Override
		public void map(Text key, ArchiveReader value, Context context) throws IOException {

			for (ArchiveRecord r : value) {
				try {
					// We're only interested in processing the responses, not requests or metadata
					if ("response".equals(r.getHeader().getHeaderValue("WARC-Type"))) {
                        context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
                        WARCDoc doc = new WARCDoc(r);
                        if (doc.isFeed()) {
                            context.getCounter(MAPPERCOUNTER.FEEDS_IN).increment(1);
                            if (doc.isGeoRss()) {
                                context.getCounter(MAPPERCOUNTER.GEO_RSS_IN).increment(1);
                                outKey.set(r.getHeader().getUrl());
                                vals[0] = doc.getUpdatedAt();
                                vals[1] = doc.countGeoTags();
                                vals[2] = doc.countLocations();
                                //LOG.info("Writing "+outKey+"\t"+vals[0]+"\t"+vals[1]+"\t"+vals[2]);
                                result.set(vals);
                                context.write(outKey, result);
                            }
                        }
					}
				}
				catch (Exception ex) {
					LOG.error("Caught Exception", ex);
					context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
				}
			}
		}
	}

    /**
     * Reduces from an array of longs to an array of sums of those longs, formatted as a tab-delimited string.
     */
    public static class ArrayLongSumReducer extends Reducer<Text, ArrayPrimitiveWritable, Text, Text> {
        private Text result = new Text();
        StringBuilder sb = new StringBuilder();

        @Override
        public void reduce(Text key, Iterable<ArrayPrimitiveWritable> values,
                           Context context) throws IOException, InterruptedException {
            long[] sums = null;
            for (ArrayPrimitiveWritable val : values) {
                long[] vals = (long[]) val.get();
                //LOG.info("Reducing "+key+"\t"+vals[0]+"\t"+vals[1]+"\t"+vals[2]);
                if (sums == null) {
                    sums = vals;
                } else {
                    for (int i = 0; i < sums.length && i < vals.length; ++i) {
                        sums[i] = sums[i] + vals[i];
                    }
                }
            }
            if (sums == null) {
                return;
            }
            sb.setLength(0);
            for (long l: sums) {
                sb.append(l).append('\t');
            }
            sb.setLength(sb.length() - 1);
            result.set(sb.toString());
            context.write(key, result);
        }
    }

    /**
     * Compares output key Text objects, first by length (shortest first), then
     * by default String compareTo.  Used for the Map-Reduce sort phase.
     */
    public static class ShortestTextComparator extends Text.Comparator {
        @Override
        public int compare(byte[] b1, int i1, int l1, byte[] b2, int i2, int l2) {
            String s1, s2;
            try {
                s1 = Text.decode(b1, i1, l1);
            } catch (CharacterCodingException ex) {
                s1 = null;
            }
            try {
                s2 = Text.decode(b2, i2, l2);
            } catch (CharacterCodingException ex) {
                s2 = null;
            }
            return compareShortest(s1, s2);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String s1 = a.toString();
            String s2 = b.toString();
            return compareShortest(s1, s2);
        }

        public int compareShortest(String s1, String s2) {
            if (s1 == null) {
                return s2 == null ? 0 : -1;
            } else if (s2 == null) {
                return 1;
            }
            int lc = s1.length() - s2.length();
            return lc != 0 ? lc : s1.compareTo(s2);
        }
    }

    /**
     * Main entry point that uses the {@link org.apache.hadoop.util.ToolRunner} class to run the Hadoop job.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GeoRSSCounter(), args);
        System.exit(res);
    }

    /**
     * Builds and runs the Hadoop job.
     * @return	0 if the Hadoop job completes successfully and 1 otherwise.
     */
    public int run(String[] args) throws Exception {
        BasicConfigurator.configure();
        // Input path example:
        //    s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2014-23/segments/1405997884827.82/warc/CC-MAIN-20140722025804-00091-ip-10-33-131-23.ec2.internal.warc.gz
        // Can accept wildcards, e.g.:
        //   s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2014-23/segments/1405997884827.82/warc/*.warc.gz
        // or even:
        //   s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2014-23/*.warc.gz
        String inputPath = args[0];
        // Output path example: s3n://cc-georss-gear11/out
        String outputPath = args[1];

        Configuration conf = getConf();
        Job job = new Job(conf, "georss");
        job.setJarByClass(GeoRSSCounter.class);
        job.setNumReduceTasks(1);

        LOG.info("Input path: " + inputPath);
        LOG.info("Output path: " + outputPath);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(WARCFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArrayPrimitiveWritable.class);

        job.setMapperClass(GeoRSSCounterMapper.class);
        job.setSortComparatorClass(ShortestTextComparator.class);
        job.setReducerClass(ArrayLongSumReducer.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
