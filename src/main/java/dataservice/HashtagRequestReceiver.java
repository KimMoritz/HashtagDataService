package dataservice;

import dataservice.mongo.dao.MongoClientHandler;
import dataservice.mongo.dao.MongoDAO;
import dataservice.mongo.dao.hashtags.HashtagongoDAOFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.json.JSONObject;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import javax.jms.*;

@Component
public class HashtagRequestReceiver{
    @JmsListener(destination = "mongoServiceQueue", containerFactory = "jmsListenerContainerFactory")
    public void receiveMessage(TextMessage textMessage){
        try{
            System.out.println("received: " + textMessage.getText() + "\n\treply to: " + textMessage.getJMSReplyTo());
            JSONObject mongoReply = getFromMongo(textMessage);
            sendReply(textMessage, mongoReply.toString());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private JSONObject getFromMongo(TextMessage textMessage){
        MongoDAO mongoDAO = HashtagongoDAOFactory.mongoDAO();
        JSONObject response;
        try {
            JSONObject jsonObject = new JSONObject(textMessage.getText());
            String period = jsonObject.getString("period");
            String hashtag = jsonObject.getString("hashtag");
            if (period != null && hashtag != null){
                response = mongoDAO.getHashtags(period, hashtag, MongoClientHandler.getMongoClient());
                if (response != null){
                    return response;
                }else throw (new MongoDAOException("No response from MongoDAO"));
            }else throw (new JMSException("Missing period and/or hashtag parameter in request JSON String."));
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (MongoDAOException e){
            e.printStackTrace();
            System.out.println("Could not retrieve from database.");
            //TODO: Add logger here.
        }
        return null;
    }

    private void sendReply(TextMessage textMessage, String body) {
        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
            Connection connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Destination returnDestination = textMessage.getJMSReplyTo();
            MessageProducer prod = session.createProducer(returnDestination);
            TextMessage textMessage1 = createReplyTextMessage(textMessage, body, session);
            prod.send(textMessage1);
            System.out.println("reply " + textMessage1.getText() +"\nsent to : " + returnDestination);

            prod.close();
            session.close();
            connection.stop();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private TextMessage createReplyTextMessage(TextMessage requestTextMessage, String body, Session session) {
        TextMessage replyTextMessage = null;
        try {
            replyTextMessage = session.createTextMessage(body);
            replyTextMessage.setJMSCorrelationID(requestTextMessage.getJMSCorrelationID());
            replyTextMessage.setJMSDestination(requestTextMessage.getJMSDestination());
        } catch (JMSException e) {
            e.printStackTrace();
        }

        return replyTextMessage;
    }
}
