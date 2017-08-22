package dataservice;

import dataservice.mongo.dao.MongoClientHandler;
import dataservice.mongo.dao.MongoDAO;
import dataservice.mongo.dao.hashtags.HashtagongoDAOFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.util.Date;
import java.util.ResourceBundle;

@Component
public class HashtagFetcher {
    @JmsListener(destination = "hashtagMongoQueue", containerFactory = "jmsListenerContainerFactory")
    public void receiveMessage(TextMessage textMessage){
        MongoDAO fetcherMongoDao2 = HashtagongoDAOFactory.mongoDAO();
        if(textMessage != null){
            try {
                fetcherMongoDao2.insertHashtagOccurrence(textMessage.getText(), MongoClientHandler.getMongoClient(), new Date());
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    private Connection connection;

    public HashtagFetcher(){
        ResourceBundle resourceBundle = ResourceBundle.getBundle("application");
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(resourceBundle.getString("spring.activemq.broker-url"));
        try {
            connection = activeMQConnectionFactory.createConnection();
            connection.start();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void receiveAndInsert(){
        TextMessage textMessage = fetchTextMessage();
        MongoDAO fetcherMongoDao2 = HashtagongoDAOFactory.mongoDAO();
        if(textMessage != null){
            try {
                fetcherMongoDao2.insertHashtagOccurrence(textMessage.getText(), MongoClientHandler.getMongoClient(), new Date());
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    private TextMessage fetchTextMessage() {
        TextMessage response = null;
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("hashtagMongoQueue");
            MessageConsumer consumer = session.createConsumer(destination);
            response = (TextMessage) consumer.receive(1000);
            session.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return response;
    }
}
