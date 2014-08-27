package com.gear11.warc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.archive.io.ArchiveRecord;
import org.apache.log4j.Logger;


/**
 * Wraps a WARC archive record to pull out information about the HTTP response/document.
 * TODO: Add support for wgs84_pos
 */
public class WARCDoc {

    protected static final Logger LOG = Logger.getLogger(WARCDoc.class);
    protected static final XMLInputFactory factory = XMLInputFactory.newInstance();

    protected final int statusCode;
    protected final Map<String,String> headers = new HashMap<String,String>();
    protected String mimeType;
    protected String charset;
    protected BufferedReader reader;
    protected Set<String> namespaces;
    protected XMLStreamReader xmlStreamReader;
    //protected String geoRssNs;

    // State captured during parse
    protected boolean isParsed;
    protected boolean _isGeoRSS;
    protected int geoTagCount;
    protected Set<Integer> locHashes = new HashSet<Integer>();
    protected long mostRecentEpochSec = -1;

    // MIME types that mean the response is possibly an RSS feed.
    protected static final Set<String> FEED_MIME_TYPES = new HashSet<String>(Arrays.asList(
            "text/xml",
            "text/rss+xml",
            "application/rss+xml"
    ));

    // Elements that may contain dates
    protected static final Set<String> DATE_ELS = new HashSet<String>(Arrays.asList(
            "pubDate",
            "updated",
            "published"
    ));

    /**
     * Constructs a WARCDoc wrapper for the given record.
     */
    public WARCDoc(ArchiveRecord r) throws IOException {
        reader = new BufferedReader(new InputStreamReader(r, "utf-8"));

        // Parse out HTTP response status
        String line = reader.readLine();
        String[] segs = line.split(" ");
        statusCode = Integer.parseInt(segs[1]);

        // Parse headers
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.length() == 0) { // Line was only whitespace
                break;
            }
            // Parse HTTP header as name: value
            int n = line.indexOf(':');
            if (n <= 0) {
                continue;
            }
            String key = line.substring(0, n).trim().toLowerCase();
            String val = line.substring(n+1).trim();
            headers.put(key, val);
            // For content-type, attempt to parse out char set
            if ("content-type".equals(key)) {
                n = val.indexOf(';');
                if (n > 0) {
                    mimeType = val.substring(0, n);
                    String extra = val.substring(n+1).trim();
                    if (extra.startsWith("charset=")) {
                        charset = extra.substring(8);
                        // Might need to start over!
                        //reader = new BufferedReader(new InputStreamReader(r, charset));
                    }
                } else {
                    mimeType = val;
                }
            }
        }
    }

    /**
     * Returns true iff this document appears to be a feed MIME type.
     */
    public boolean isFeed() {
        return FEED_MIME_TYPES.contains(getMimeType());
    }

    /**
     * Returns the set of header names discovered on the response.
     */
    public Set<String> headerNames() {
        return headers.keySet();
    }

    /**
     * Retrieves the header by (case-insensitive) name
     */
    public String getHeader(String name) {
        return headers.get(name.toLowerCase());
    }

    /**
     * Returns this document's MIME type.
     */
    public String getMimeType() {
        return mimeType;
    }

    /**
     * Returns this document's charset
     */
    public String getCharset() {
        return charset;
    }

    /**
     * Returns this document's length
     */
    public int getContentLength() {
        String s = getHeader("content-length");
        if (s == null) {
            return -1;
        }
        return Integer.parseInt(s);
    }

    /**
     * Returns a reader for this document's content.
     */
    public BufferedReader getReader() {
        if (xmlStreamReader != null) {
            throw new IllegalStateException("Already started XML parse");
        }
        return reader;
    }

    /**
     * Returns an XML stream reader for this document's content.
     */
    protected XMLStreamReader getXMLStreamReader() throws XMLStreamException {
        if (xmlStreamReader == null) {
            // Must prevent parser from attempting to validate external
            // DTDs or process may halt.
            factory.setProperty("javax.xml.stream.supportDTD", false);
            xmlStreamReader = factory.createXMLStreamReader(reader);
        }
        return xmlStreamReader;
    }

    /**
     * Returns the number of Geo RSS tags discovered in the record.
     */
    public int countGeoTags() {
        parseXml();
        return this.geoTagCount;
    }

    /**
     * Returns true iff this archive is an XML document with a GeoRSS namespace.
     */
    public boolean isGeoRss() {
        parseXml();
        return this._isGeoRSS;
    }

    /**
     * Returns the number of distinct locations found
     */
    public int countLocations() {
        parseXml();
        return this.locHashes.size();
    }

    public long getUpdatedAt() {
        parseXml();
        return this.mostRecentEpochSec;
    }

    protected synchronized void parseXml() {
        if (this.isParsed) {
            return;
        }
        this.isParsed = true;
        // Nothing to parse from a non-feed
        if (!this.isFeed()) {
            return;
        }
        // Determine if this is GeoRSS
        String geoRssNs = null;
        try {
            for (String ns : getNamespaces()) {
                if (ns.indexOf("georss") > 0) {
                    geoRssNs = ns;
                    break;
                }
            }
        } catch (XMLStreamException ex) {
            // Ignore
        }
        this._isGeoRSS = geoRssNs != null;
        // An optimization--we only care about GeoRSS feeds.  Remove if this changes.
        if (!this._isGeoRSS) {
            return;
        }
        // We have a GeoRSS feed.  Look for update time and locations
        try {
            XMLStreamReader reader = getXMLStreamReader();
            while (reader.hasNext()) {
                int event = reader.next();
                switch (event) {
                    case XMLStreamConstants.START_ELEMENT:
                        String ns = reader.getNamespaceURI();
                        if (geoRssNs.equals(ns)) {
                            String text = reader.getElementText();
                            //LOG.info("GeoRSS string:  "+text);
                            locHashes.add(text.hashCode());
                            ++this.geoTagCount;
                        }
                        if (DATE_ELS.contains(reader.getLocalName())) {
                            String text = reader.getElementText();
                            //LOG.info("Date string:  "+text);
                            long epochSec = DateHelper.toEpochSec(text);
                            this.mostRecentEpochSec = Math.max(this.mostRecentEpochSec, epochSec);
                        }
                }
            }
        } catch (XMLStreamException ex) {
            // Ignore
        }
    }


    /**
     * Parses this archive as XML, until the first element with
     * one or more namespaces is parsed.
     */
    public Set<String> getNamespaces() throws XMLStreamException {
        if (namespaces == null) {
            namespaces = new HashSet<String>();
            XMLStreamReader reader = getXMLStreamReader();
            int x = 0;
            while (x < 100 && reader.hasNext() && namespaces.isEmpty()) {
                x += 1;
                int event = reader.next();
                switch (event) {
                    case XMLStreamConstants.START_ELEMENT:
                        int count = reader.getNamespaceCount();
                        for (int i = 0; i < count; ++i) {
                            namespaces.add(reader.getNamespaceURI(i));
                        }
                }
            }
        }
        return namespaces;
    }

    public static class DateHelper {


        // RSS:
        // http://validator.w3.org/feed/docs/rss2.html
        // <pubDate>Mon, 25 Aug 2014 07:07:58 +0000</pubDate>
        //
        // Atom:
        // http://tools.ietf.org/html/rfc4287#page-10
        /*
        <updated>2003-12-13T18:30:02Z</updated>
        <updated>2003-12-13T18:30:02.25Z</updated>
        <updated>2003-12-13T18:30:02+01:00</updated>
        <updated>2003-12-13T18:30:02.25+01:00</updated>
        */
        protected static final SimpleDateFormat RSS_FORMAT = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");
        protected static final SimpleDateFormat ATOM_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");

        /**
         * Attempts to parse some of the various date formats encountered in RSS
         * feeds and return a Unix seconds-since-epoch timestamp.
         *
         * Returns -1 if the date format was not understood.
         */
        public static long toEpochSec(String dateStr) {
            // RSS:
            // http://validator.w3.org/feed/docs/rss2.html
            // Sat, 07 Sep 2002 0:00:01 GMT
            // <pubDate>Mon, 25 Aug 2014 07:07:58 +0000</pubDate>
            //
            // Atom:
            // http://tools.ietf.org/html/rfc4287#page-10
            /*
            <updated>2003-12-13T18:30:02Z</updated>
            <updated>2003-12-13T18:30:02.25Z</updated>
            <updated>2003-12-13T18:30:02+01:00</updated>
            <updated>2003-12-13T18:30:02.25+01:00</updated>
            */
            // Invalid date
            if (dateStr == null || dateStr.length() < 10) {
                return -1;

            // If length is 10, assume YYYY-MM-DD only and fix to midnight
            } else if (dateStr.length() == 10) { // YYYY-MM-DD only
                dateStr = dateStr.replace('/', '-');
                dateStr += "T12:00:00";
            }

            // Atom format
            if (dateStr.charAt(10) == 'T') {
                // Throw out subseconds
                int n = dateStr.indexOf('.');
                if (n > 0) {
                    // Find digits after '.'
                    int m = n + 1;
                    for ( ; m < dateStr.length(); ++m) {
                        if (!Character.isDigit(dateStr.charAt(m))) {
                            break;
                        }
                    }
                    // Re-append from first non-digit after '.'
                    if (m < dateStr.length()) {
                        dateStr  = dateStr.substring(0, n) + dateStr.substring(m);
                    } else {
                        dateStr = dateStr.substring(0, n);
                    }
                }
                // Not enough characters for time
                if (dateStr.length() < 19) {
                    return -1;
                }
                try {
                    Date date = ATOM_FORMAT.parse(dateStr);
                    return date.getTime() / 1000;
                } catch (ParseException ex) {
                    //LOG.warn("Parse exception on "+dateStr);
                    return -1;
                }
            // RSS format
            } else {
                try {
                    Date date = RSS_FORMAT.parse(dateStr);
                    return date.getTime() / 1000;
                } catch (ParseException ex) {
                    //LOG.warn("Parse exception on "+dateStr);
                    return -1;
                }
            }
        }
    }
}
