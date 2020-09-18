// camel-k: language=java


import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.json.simple.parser.*;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import java.util.Iterator;
import java.util.Map;

public class PactTranslationBridge extends RouteBuilder {

    String mmid1;
    String mmid2;
    class LocProcessor implements Processor
    {

      public void process(Exchange exchange) throws Exception
      {
          String body = exchange.getIn().getBody(String.class);
          String[] parsebody = body.split(",");
          mmid1 = parsebody[0];
          mmid2 = parsebody[1];
          String location = parsebody[2] + "," + parsebody[3];
          String mapquesturi = "www.mapquestapi.com/geocoding/v1/reverse?key=ZoGRKrEpoaLNO1aMR9rGd1I1FVg3QEVX&location=" + location + "&includeRoadMetadata=false&includeNearestIntersection=false";
          exchange.getIn().setBody(mapquesturi);
      }
    }

    class MapRespProcessor implements Processor
    {
      private int getIndex(String key)
      {
         String locmap[] = {"street","adminArea5","adminArea3","postalCode","adminArea1"};
         int idx = -1;

         for (int i=0; i < locmap.length; ++i)
         {
           if (locmap[i].equals(key))
             idx = i;
         }
         System.out.println("Returning index of " + idx + " for key " + key);
         return idx;
      }

      private String formatBody(String[] body)
      {
        String fmtBody="";
        for (int i=0; i < body.length; ++i)
        {
          if (i==0)
            fmtBody = body[i];
          else
            fmtBody = fmtBody + "," + body[i];
        }
        return fmtBody;
      }
            
         
      public void process(Exchange exchange) throws Exception
      {
        String[] retBody = new String[7];
        String body = exchange.getIn().getBody(String.class);
        System.out.println("Map Body: " + body);
        Object obj = new JSONParser().parse(body);
        JSONObject jo = (JSONObject)obj;
        JSONArray jra = (JSONArray) jo.get("results");
        Iterator it_jra = jra.iterator();
        while (it_jra.hasNext())
        {
          obj = it_jra.next();
          JSONObject jlo = (JSONObject)obj;
          JSONArray jla = (JSONArray)jlo.get("locations");
          Iterator it_jla = jla.iterator();
          while(it_jla.hasNext())
          {
            Iterator<Map.Entry>it_loc = ((Map)it_jla.next()).entrySet().iterator();
            while (it_loc.hasNext())
            {
              Map.Entry pair = it_loc.next();
              System.out.println(pair.getKey() + ":" + pair.getValue());
              int idx = getIndex(pair.getKey().toString());
              if (idx != -1)
                retBody[idx+2] = (pair.getValue().toString());
            }
          }
        }
        retBody[0] = mmid1;
        retBody[1] = mmid2;
        
        System.out.println("Body " + formatBody(retBody));
        exchange.getIn().setBody(formatBody(retBody));
      }
    }

    private final String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private final String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    @Override
    public void configure() throws Exception {
        from("kafka:pact-untranslated?brokers=pact-cluster-kafka-brokers.kafka.svc.cluster.local:9092").process(new LocProcessor())
        .log("uri ${body}")
        .toD("http://${body}").process(new MapRespProcessor())
        .log("body consumed ${body}")    
        .removeHeaders("*")
        .to("kafka:pact_topic?brokers=pact-cluster-kafka-brokers.kafka.svc.cluster.local:9092");

    }
}
