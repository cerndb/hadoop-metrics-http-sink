package ch.cern.hadoop.metrics2.sink.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Scanner;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.net.DNS;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

public abstract class AbstractHTTPMetricsSink implements MetricsSink {
    
    public final Log LOG = LogFactory.getLog(this.getClass());
    
    public static final String COLLECTOR_HOST_PROPERTY = "collector";
    private String collectorUri;
    
    public static final String AUTH_PROPERTY = "auth";
    public static final String AUTH_USERNAME_PROPERTY = AUTH_PROPERTY + ".user";
    public static final String AUTH_PASSWORD_PROPERTY = AUTH_PROPERTY + ".password";

    protected String hostName = "UNKNOWN.example.com";
    
    private CloseableHttpClient httpClient;

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
        
        // Authentication configs
        boolean authentication = conf.getBoolean(AUTH_PROPERTY, false);
        if(authentication){
            String username = conf.getString(AUTH_USERNAME_PROPERTY);
            String password = conf.getString(AUTH_PASSWORD_PROPERTY);
            
            CredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
            provider.setCredentials(AuthScope.ANY, credentials);
            
            httpClient = HttpClientBuilder.create()
                    .setDefaultCredentialsProvider(provider)
                    .build();
            
            LOG.info("Collector Uri: " + collectorUri + " (authentication enabled, user: " + username + ")");
        }else{
            httpClient = HttpClientBuilder.create().build();
            
            LOG.info("Collector Uri: " + collectorUri);
        }
    }

    protected void emitMetrics(HTTPMetric metrics) throws IOException {
        String connectUrl = getCollectorUri();
        LOG.debug("connectUrl: " + connectUrl);
        try {
            String content = metrics.toJSON();
            LOG.debug("Json HTTP metrics: " + content);
            
            HttpPost postMethod = new HttpPost(connectUrl);
            postMethod.addHeader("Content-Type", "application/json");
            postMethod.setEntity(new StringEntity(content, "UTF-8"));
            
            CloseableHttpResponse response = httpClient.execute(postMethod);
            
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 201) {
                LOG.error("Unable to POST metrics to collector=" + connectUrl + " with status code=" + statusCode
                        + "\n Response: " + response
                        + "\n Response content: " + convertStreamToString(response.getEntity().getContent()));
            } else {
                LOG.debug("Metrics posted to Collector " + connectUrl);
            }
        } catch (ConnectException e) {
            throw new UnableToConnectException(e).setConnectUrl(connectUrl);
        }
    }
    
    static String convertStreamToString(InputStream is) {
        @SuppressWarnings("resource")
        Scanner s = new Scanner(is).useDelimiter("\\A");
        
        return s.hasNext() ? s.next() : "";
    }

    public void setHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    protected String getCollectorUri() {
        return collectorUri;
    }

    public void flush() {
        // nothing to do as we are not buffering data
    }
}
