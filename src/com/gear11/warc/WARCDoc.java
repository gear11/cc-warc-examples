package com.gear11.warc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.archive.io.ArchiveRecord;

/**
 * Wraps a WARC archive record to pull out information about the HTTP response/document.
 * TODO: Add support for wgs84_pos
 */
public class WARCDoc {
    protected final int statusCode;
    protected final Map<String,String> headers = new HashMap<String,String>();
    protected String mimeType;
    protected String charset;
    protected BufferedReader reader;
    protected Set<String> namespaces;
    protected XMLStreamReader xmlStreamReader;
    protected String geoRssNs;

    // MIME types that mean the response is possibly an RSS feed.
    protected static final Set<String> FEED_MIME_TYPES = new HashSet<String>(Arrays.asList(
            "text/xml",
            "text/rss+xml",
            "application/rss+xml"
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
    protected BufferedReader getReader() {
        if (xmlStreamReader != null) {
            throw new IllegalStateException("Already started XML parse");
        }
        return reader;
    }

    /**
     * Returns an XML stream reader for this document's content.
     */
    public XMLStreamReader getXMLStreamReader() throws XMLStreamException {
        if (xmlStreamReader == null) {
            XMLInputFactory factory = XMLInputFactory.newInstance();
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
        if (!isGeoRss()) {
            return 0;
        }
        int tags = 0;
        try {
            XMLStreamReader reader = getXMLStreamReader();
            while (reader.hasNext()) {
                int event = reader.next();
                switch (event) {
                    case XMLStreamConstants.START_ELEMENT:
                        String ns = reader.getNamespaceURI();
                        if (geoRssNs.equals(ns)) {
                            ++tags;
                        }
                }
            }
        } catch (XMLStreamException ex) {
            // Ignore
        }
        return tags;
    }

    /**
     * Returns true iff this archive is an XML document with a GeoRSS namespace.
     */
    public boolean isGeoRss() {
        if (geoRssNs != null) {
            return true;
        }
        if (!isFeed()) {
            return false;
        }
        try {
            for (String ns : getNamespaces()) {
                if (ns.indexOf("georss") > 0) {
                    //System.out.println(ns);
                    geoRssNs = ns;
                    return true;
                }
            }
        } catch (XMLStreamException ex) {
            // Ignore
        }
        return false;
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

}
