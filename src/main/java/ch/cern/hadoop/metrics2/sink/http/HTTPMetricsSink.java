package ch.cern.hadoop.metrics2.sink.http;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.AbstractMetric;

import java.io.IOException;
import java.util.*;

public class HTTPMetricsSink extends AbstractHTTPMetricsSink {
    
    public static final String TAGS_PARAM = "tags";
    private Map<String, String> tags;

    @Override
    public void init(SubsetConfiguration conf) {
        super.init(conf);
        
        tags = parseTags(conf);
    }

    private Map<String, String> parseTags(SubsetConfiguration conf) {
        HashMap<String, String> tags = new HashMap<String, String>();
        
        String[] tags_splitted = conf.getString(TAGS_PARAM).split(" ");
        
        for (String tag : tags_splitted) {
            String key = tag;
            String value = conf.getString(TAGS_PARAM + "." + key);
            
            tags.put(key, value);
        }
        
        return tags;
    }

    public void putMetrics(MetricsRecord record) {
        try {
            String context = record.context() + "." + record.name();
            Collection<AbstractMetric> metrics = (Collection<AbstractMetric>) record.metrics();

            HTTPMetric httpMetric = new HTTPMetric();
            httpMetric.setUpdateTime(record.timestamp());
            httpMetric.setHostName(hostName);
            httpMetric.setContext(context);
            httpMetric.setTags(tags);

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
    
    public Map<String, String> getTags(){
        return tags;
    }
    
}
