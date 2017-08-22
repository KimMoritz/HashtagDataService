package dataservice.mongo.dao;

import com.mongodb.MongoClient;
import org.json.JSONObject;

import java.util.Date;

public interface MongoDAO {

    void insertHashtagOccurrence(String hashTagJsonString, MongoClient mongoClient, Date timestamp);

    boolean insertSubscriptionHashtag(String value, String subscriber, MongoClient mongoClient);

    JSONObject getHashtags(String period, String hashtag, MongoClient mongoClient);

    void performMaintenance(MongoClient mongoClient);

}
