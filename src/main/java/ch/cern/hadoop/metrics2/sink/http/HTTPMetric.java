package ch.cern.hadoop.metrics2.sink.http;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class HTTPMetric  {
    
    @JsonIgnore
    protected static ObjectMapper mapper = new ObjectMapper();
    
    @JsonIgnore //This date format is automatically interpreted as date by Elastic
    public static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    private String updateTime;
    private String hostName;
    private String context;
    
    private Map<String, String> metrics;
    
    @JsonIgnore
    private Map<String, String> extraAttributes;

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime_ms) {
        this.updateTime = DATE_FORMAT.format(new Date(updateTime_ms));
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    @JsonIgnore
    public Map<String, String> getExtraAttributes() {
        return extraAttributes;
    }

    @JsonIgnore
    public void setExtraAttributes(Map<String, String> extraAttributes) {
        this.extraAttributes = extraAttributes;
    }

    public Map<String, String> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, String> metrics) {
        this.metrics = metrics;
    }

    public String toJSON() throws JsonGenerationException, JsonMappingException, IOException {
        ObjectNode node = mapper.valueToTree(this);
        
        if(extraAttributes != null)
            for (Map.Entry<String, String> entry : extraAttributes.entrySet())
                node.put(entry.getKey(), entry.getValue());
        
        return node.toString();
    }
    
}
