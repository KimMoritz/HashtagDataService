package dataservice.mongo.dao.hashtags;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import dataservice.mongo.dao.MongoDAO;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class HashtagMongoDAOTest {

    MongoClient mockMongoClient;
    MongoDatabase mockMongoDatabase;
    MongoCollection mockMongoCollection;
    MongoCollection mockSubscriptionCollection;

    @Before
    public void setUp() throws Exception {
        mockMongoClient = mock(MongoClient.class);
        mockMongoDatabase = mock(MongoDatabase.class);
        mockMongoCollection = mock(MongoCollection.class);
        mockSubscriptionCollection = mock(MongoCollection.class);
        when(mockMongoClient.getDatabase("Hashtags")).thenReturn(mockMongoDatabase);
        when(mockMongoClient.getDatabase("SubscribedHashtags")).thenReturn(mockMongoDatabase);
        when(mockMongoDatabase.getCollection("lessthanh")).thenReturn(mockMongoCollection);
        when(mockMongoDatabase.getCollection("subscription_hashtags")).thenReturn(mockSubscriptionCollection);
    }

    @Test
    public void insertHashtagOccurrence() throws Exception {
        MongoDAO hashtagMongoDao2 = HashtagongoDAOFactory.mongoDAO();
        Date timestamp = new Date();
        String jsonObjectString = new JSONObject()
                .put("key", "Brexit")
                .put("value", 4)
                .toString();
        hashtagMongoDao2.insertHashtagOccurrence(jsonObjectString, mockMongoClient, timestamp);
        Mockito.verify(mockMongoCollection).insertOne(any());
    }

    /*@Test
    public void insertSubscriptionHashtagTest() throws Exception {
        assertTrue("Should return true",
                new HashtagMongoDAO().insertSubscriptionHashtag("Brexit", "Company A", mockMongoClient));
        Mockito.verify(mockMongoCollection).insertOne(any());
    }*/

    @Test
    public void getHashtags() throws Exception {
        MongoDAO mongoDAO = HashtagongoDAOFactory.mongoDAO();
        JSONObject jsonObject = mongoDAO.getHashtags("week", "Trump", mockMongoClient);
        assertNotNull("Should not be null", jsonObject);
        assertEquals("Should return JSONObject", JSONObject.class, jsonObject.getClass());/*
        assertEquals("Should have an xvals String",String.class, jsonObject.get("xvals").getClass());
        assertEquals("Should have an yvals String",String.class, jsonObject.get("yvals").getClass());*/
    }

    @Test
    public void performMaintenance() throws Exception {
    }

}