package ch.cern.hadoop.metrics2.sink.http;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.AbstractMetric;

import java.io.IOException;
import java.util.*;

public class HTTPMetricsSink extends AbstractHTTPMetricsSink {
    
    public final Log LOG = LogFactory.getLog(this.getClass());
    
    public static final String EXTRA_ATTS_PARAM = "extraAttributes";
    private Map<String, String> extraAttributes;

    @Override
    public void init(SubsetConfiguration conf) {
        super.init(conf);
        
        extraAttributes = parsEextraAttributes(conf);
        
        LOG.info("Initialized with extra attributes: " + extraAttributes);
    }

    private Map<String, String> parsEextraAttributes(SubsetConfiguration conf) {
        HashMap<String, String> extra_att = new HashMap<String, String>();
        
        String extra_att_string = conf.getString(EXTRA_ATTS_PARAM);
        if(extra_att_string == null)
            return extra_att;
        
        String[] extra_att_splitted = extra_att_string.split(" ");
        
        for (String extra_attribute : extra_att_splitted) {
            String key = extra_attribute;
            String value = conf.getString(EXTRA_ATTS_PARAM + "." + key);
            
            extra_att.put(key, value);
        }
        
        return extra_att;
    }

    public void putMetrics(MetricsRecord record) {
        try {
            String context = record.context() + "." + record.name();
            Collection<AbstractMetric> metrics = (Collection<AbstractMetric>) record.metrics();

            HTTPMetric httpMetric = new HTTPMetric();
            httpMetric.setUpdateTime(record.timestamp());
            httpMetric.setHostName(hostName);
            httpMetric.setContext(context);
            httpMetric.setExtraAttributes(extraAttributes);

            Map<String, String> metricMap = new HashMap<String, String>();
            for (AbstractMetric metric : metrics) {
                metricMap.put(metric.name(), metric.value().toString());
            }
            httpMetric.setMetrics(metricMap);

            emitMetrics(httpMetric);
        } catch (UnableToConnectException uce) {
            LOG.warn("Unable to send metrics to collector by address:" + uce.getConnectUrl());
        } catch (IOException io) {
            throw new MetricsException("Failed to putMetrics", io);
        }
    }
    
    public Map<String, String> getExtraAttributes(){
        return extraAttributes;
    }
    
}
