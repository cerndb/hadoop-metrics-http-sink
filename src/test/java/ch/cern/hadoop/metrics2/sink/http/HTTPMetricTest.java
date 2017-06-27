package ch.cern.hadoop.metrics2.sink.http;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class HTTPMetricTest {
    
    @Test
    public void toJSON() throws JsonGenerationException, JsonMappingException, IOException{
        
        HTTPMetric metric = new HTTPMetric();
        
        long currentTime = System.currentTimeMillis();
        
        metric.setUpdateTime(currentTime);
        
        HashMap<String, String> extraAttributes = new HashMap<String, String>();
        extraAttributes.put("att1", "val1");
        extraAttributes.put("att2", "val2");
        extraAttributes.put("att3", "val3");
        metric.setExtraAttributes(extraAttributes);
        
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(metric.toJSON());
        JsonObject jsonParsed = element.getAsJsonObject();
        System.out.println(metric.toJSON());
        Assert.assertEquals(HTTPMetric.DATE_FORMAT.format(new Date(currentTime)), 
                            jsonParsed.get("updateTime").getAsString());
        
        Assert.assertEquals("val1", jsonParsed.get("att1").getAsString());
        Assert.assertEquals("val2", jsonParsed.get("att2").getAsString());
        Assert.assertEquals("val3", jsonParsed.get("att3").getAsString());
    }
    
}
