package ch.cern.hadoop.metrics2.sink.http;

import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class HTTPMetricsSinkTest {

    String SINK_NAME = "http";
    
    @Test
    public void parseExtraAttributes(){
        
        HTTPMetricsSink sink = new HTTPMetricsSink();
        
        BaseConfiguration conf = new BaseConfiguration();
        
        conf.addProperty(SINK_NAME + "." + AbstractHTTPMetricsSink.COLLECTOR_HOST_PROPERTY, "http://localhost:1234/index/type/");
        
        conf.addProperty(SINK_NAME + "." + HTTPMetricsSink.EXTRA_ATTS_PARAM, "machine hg env");
        conf.addProperty(SINK_NAME + "." + HTTPMetricsSink.EXTRA_ATTS_PARAM + ".machine", "123.cern.ch");
        conf.addProperty(SINK_NAME + "." + HTTPMetricsSink.EXTRA_ATTS_PARAM + ".hg", "hadoop/datanode");
        conf.addProperty(SINK_NAME + "." + HTTPMetricsSink.EXTRA_ATTS_PARAM + ".env", "prod");
        
        SubsetConfiguration subConf = new SubsetConfiguration(conf, SINK_NAME, ".");
        SubsetConfiguration.setDefaultListDelimiter(' ');
        sink.init(subConf);
        
        Map<String, String> tags = sink.getExtraAttributes();
        
        Assert.assertEquals(tags.get("machine"), "123.cern.ch");
        Assert.assertEquals(tags.get("hg"), "hadoop/datanode");
        Assert.assertEquals(tags.get("env"), "prod");
    }
    
    @Test
    public void noConfig(){
        HTTPMetricsSink sink = new HTTPMetricsSink();
        
        BaseConfiguration conf = new BaseConfiguration();
        
        conf.addProperty(SINK_NAME + "." + AbstractHTTPMetricsSink.COLLECTOR_HOST_PROPERTY, "http://localhost:1234/index/type/");
        
        SubsetConfiguration subConf = new SubsetConfiguration(conf, SINK_NAME, ".");
        SubsetConfiguration.setDefaultListDelimiter(' ');
        sink.init(subConf);
    }
}
