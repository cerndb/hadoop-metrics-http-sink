package ch.cern.hadoop.metrics2.sink.http;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.net.DNS;

public abstract class AbstractHTTPMetricsSink implements MetricsSink {
    
    public final Log LOG = LogFactory.getLog(this.getClass());
    
    public static final String COLLECTOR_HOST_PROPERTY = "collector";

    protected String hostName = "UNKNOWN.example.com";
    
    private String collectorUri;
    
    private HttpClient httpClient = new HttpClient();

    public void init(SubsetConfiguration conf) {
        LOG.info("Initializing HTTP metrics sink.");

        // Take the hostname from the DNS class.
        if (conf.getString("slave.host.name") != null) {
            hostName = conf.getString("slave.host.name");
        } else {
            try {
                hostName = DNS.getDefaultHost(
                        conf.getString("dfs.datanode.dns.interface", "default"),
                        conf.getString("dfs.datanode.dns.nameserver", "default"));
            } catch (UnknownHostException uhe) {
                LOG.error(uhe);
                hostName = "UNKNOWN.example.com";
            }
        }

        LOG.info("Identified hostname = " + hostName);

        // Load collector configs
        collectorUri = conf.getString(COLLECTOR_HOST_PROPERTY);
        if (collectorUri == null) {
            LOG.error("No Metric collector configured.");
        }

        LOG.info("Collector Uri: " + collectorUri);
    }

    protected void emitMetrics(HTTPMetric metrics) throws IOException {
        String connectUrl = getCollectorUri();
        LOG.debug("connectUrl: " + connectUrl);
        try {
            String content = metrics.toJSON();
            LOG.debug("Json HTTP metrics: " + content);

            StringRequestEntity requestEntity = new StringRequestEntity(content, "application/json", "UTF-8");
            PostMethod postMethod = new PostMethod(connectUrl);
            postMethod.setRequestEntity(requestEntity);
            int statusCode = httpClient.executeMethod(postMethod);
            if (statusCode != 201) {
                LOG.error("Unable to POST metrics to collector=" + connectUrl + " with status code=" + statusCode);
            } else {
                LOG.debug("Metrics posted to Collector " + connectUrl);
            }
        } catch (ConnectException e) {
            throw new UnableToConnectException(e).setConnectUrl(connectUrl);
        }
    }

    public void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    protected String getCollectorUri() {
        return collectorUri;
    }

    public void flush() {
        // nothing to do as we are not buffering data
    }
}
