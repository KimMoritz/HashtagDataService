package dataservice;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

public class HashtagRequestReceiverTest {
    private HashtagRequestReceiver hashtagRequestReceiver;
    private String incomingHashtagExample;
    private String incomingRequestExample;
    private String invalidIncomingRequestExample;
    private String hashtag;

    @Before
    public void setUp() throws Exception {
        hashtagRequestReceiver = new HashtagRequestReceiver();
        incomingHashtagExample = "{\"key\": \"test\", \"value\":1}";
        incomingRequestExample = "{\"period\": \"week\", \"hashtag\":Trump}";
        invalidIncomingRequestExample = "{period\": \"week\", \"hashtag\":Trump}";
        hashtag = "brexit";
    }

    @Test
    public void receiveMessage() throws Exception {
    }

    @Test
    public void getFromMongoshouldReturnJson(){
        try {
            Method method = HashtagRequestReceiver.class.getDeclaredMethod("getFromMongo", TextMessage.class);
            method.setAccessible(true);
            TextMessage textMessage = new ActiveMQTextMessage();
            textMessage.setText(incomingRequestExample);
            JSONObject mongoresponse = (JSONObject) method.invoke(hashtagRequestReceiver, textMessage);
            System.out.println(mongoresponse);
            assertEquals(mongoresponse.getClass(), JSONObject.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}