package ch.cern.hadoop.metrics2.sink.http;

import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class HTTPMetricsSinkTest {

    String SINK_NAME = "http";
    
    @Test
    public void parseTags(){
        
        HTTPMetricsSink sink = new HTTPMetricsSink();
        
        BaseConfiguration conf = new BaseConfiguration();
        
        conf.addProperty(SINK_NAME + "." + AbstractHTTPMetricsSink.COLLECTOR_HOST_PROPERTY, "http://localhost:1234/index/type/");
        
        conf.addProperty(SINK_NAME + "." + HTTPMetricsSink.TAGS_PARAM, "machine hg env");
        conf.addProperty(SINK_NAME + "." + HTTPMetricsSink.TAGS_PARAM + ".machine", "123.cern.ch");
        conf.addProperty(SINK_NAME + "." + HTTPMetricsSink.TAGS_PARAM + ".hg", "hadoop/datanode");
        conf.addProperty(SINK_NAME + "." + HTTPMetricsSink.TAGS_PARAM + ".env", "prod");
        
        SubsetConfiguration subConf = new SubsetConfiguration(conf, SINK_NAME, ".");
        SubsetConfiguration.setDefaultListDelimiter(' ');
        sink.init(subConf);
        
        Map<String, String> tags = sink.getTags();
        
        Assert.assertEquals(tags.get("machine"), "123.cern.ch");
        Assert.assertEquals(tags.get("hg"), "hadoop/datanode");
        Assert.assertEquals(tags.get("env"), "prod");
    }
    
}
