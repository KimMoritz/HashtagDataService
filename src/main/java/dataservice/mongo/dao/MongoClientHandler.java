package dataservice.mongo.dao;

import com.mongodb.MongoClient;
import org.springframework.stereotype.Component;

@Component
public class MongoClientHandler {
    private static final MongoClient mongoClient = new MongoClient();

    public static synchronized MongoClient getMongoClient(){
        return mongoClient;
    }
}
