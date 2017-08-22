package dataservice.mongo;

import com.mongodb.MongoClient;
import dataservice.mongo.dao.MongoClientHandler;
import org.junit.Test;

import static org.junit.Assert.*;

public class MongoClientHandlerTest {

    @Test
    public void getMongoClient() throws Exception {
        assertEquals(MongoClient.class, MongoClientHandler.getMongoClient().getClass());
    }

}