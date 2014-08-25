package com.gear11.warc;

import java.io.FileInputStream;
import java.io.IOException;
import javax.xml.stream.XMLStreamException;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import com.gear11.warc.WARCDoc;

/**
 * An example of processing a WARC file to discover GeoRSS, based on
 * cc-warc-examples by Stephen Merity (Smerity).
 *
 * @author Andy Jenkins (@gear11sw)
 */
public class WARCDocReaderTest {

	public static void main(String[] args) throws IOException, XMLStreamException {
		// Set up a local compressed WARC file for reading 
		String fn = "data/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.gz";
		FileInputStream is = new FileInputStream(fn);
		// The file name identifies the ArchiveReader and indicates if it should be decompressed
		ArchiveReader ar = WARCReaderFactory.get(fn, is, true);
		// Once we have an ArchiveReader, we can work through each of the records it contains
		int i = 0;
		for(ArchiveRecord r : ar) {
            i += 1;
            // If we find a Geo RSS document, print the URL and how may entries it has
            if ("response".equals(r.getHeader().getHeaderValue("WARC-Type"))) {
                WARCDoc doc = new WARCDoc(r);
                if (doc.isGeoRss()) {
                    System.out.println(r.getHeader().getUrl()+","+doc.countGeoTags());
                }
            }
		}
        System.out.println(""+i+" records processed");
	}
}