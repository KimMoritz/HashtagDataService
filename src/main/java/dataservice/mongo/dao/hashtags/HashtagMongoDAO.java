package dataservice.mongo.dao.hashtags;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import dataservice.mongo.dao.MongoDAO;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

class HashtagMongoDAO implements MongoDAO {

    private MongoClient mongoClient;

    @Override
    public void insertHashtagOccurrence(String hashTagJsonString, MongoClient mongoClient, Date timestamp) {
        this.mongoClient = mongoClient;
        MongoDatabase mongoDatabase = getDatabase();
        insertOccurence(hashTagJsonString, mongoDatabase, timestamp);
    }

    private void insertOccurence(String hashTagJsonString, MongoDatabase mongoDatabase ,Date timestamp) {
        JSONObject jsonObject = new JSONObject(hashTagJsonString);
        String hashtag = jsonObject.getString("key");
        int occurrences = jsonObject.getInt("value");
        MongoCollection mongoCollection = mongoDatabase.getCollection("lessthanh");
        Document innerdocument = new Document()
                .append("hashtag", hashtag)
                .append("occurrences", occurrences);
        List <Document> hashtags = new ArrayList<>();
        hashtags.add(innerdocument);
        Document outerDocument = new Document()
                .append("timestamp", timestamp)
                .append("hashtags", hashtags);
        mongoCollection.insertOne(outerDocument);
    }

    @Override
    public boolean insertSubscriptionHashtag(String value, String subscriber, MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        try{
            MongoDatabase mongoDatabase = getDatabase();
            MongoCollection mongoCollection = mongoDatabase.getCollection("SubscribedHashtags");
            mongoCollection.insertOne( new Document("subscribed_hashtag", value).append("subscriber", subscriber));
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public JSONObject getHashtags(String period, String hashtag, MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        MongoDatabase mongoDatabase = getDatabase();
        int nUnits = getNUnits(period);
        MongoCollection mongoCollection = mongoDatabase.getCollection("greaterthanh");
        Date date = new Date();
        date.setTime(date.getTime()-nUnits*60*60*1000);
        BasicDBObject query = new BasicDBObject( "timestamp",new BasicDBObject("$gte",date));
        MongoCursor mongoCursor = mongoCollection.find(query).iterator();
        JSONArray xvals = new JSONArray();
        JSONArray yvals = new JSONArray();
        populateXY(mongoCursor, hashtag, xvals, yvals);
        JSONObject reply = new JSONObject()
                .put("xvals", xvals)
                .put("yvals", yvals);
        return reply;
    }

    private void populateXY(MongoCursor mongoCursor, String hashtag, JSONArray xvals, JSONArray yvals) {
        try{
            while (mongoCursor.hasNext()){
                Document document = (Document) mongoCursor.next();
                ArrayList<Document> arrayList = (ArrayList<Document>) document.get("hashtags");
                if(arrayList != null){
                    for(Document d: arrayList){
                        if(d.getString("hashtag").equals(hashtag)){
                            Date date = (Date) document.get("timestamp");
                            Date now = new Date();
                            long timeAgo = now.getTime() - date.getTime();
                            timeAgo = timeAgo/(60*60*1000);
                            xvals.put(timeAgo);
                            int occurrences = (int) d.get("occurrences");
                            yvals.put(occurrences);
                        }
                    }
                }
            }
        }finally {
            mongoCursor.close();
        }
    }

    private int getNUnits(String period) {
        switch (period){
            case "hour" : return  1;
            case "day" : return 24;
            case "week": return 7*24;
            case "month": return 30*7*24;
            case "year": return 12*30*7*24;
            default:return 0;
        }
    }

    @Override
    public void performMaintenance(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        MongoDatabase mongoDatabase = getDatabase();
        MongoCollection lessthanh = mongoDatabase.getCollection("lessthanh");
        MongoCollection greaterthanh = mongoDatabase.getCollection("greaterthanh");
        aggregateDocuments(lessthanh);
        moveToHour(lessthanh, greaterthanh);
        aggregateHashtags(greaterthanh);
    }

    private void aggregateDocuments(MongoCollection lessthanh) {
        MongoCursor outerMongoCursor = lessthanh.find().iterator();

        while(outerMongoCursor.hasNext()){
            Document curr = (Document) outerMongoCursor.next();
            List hashtags = (ArrayList) curr.get("hashtags");
            Document document = (Document) hashtags.get(0);
            String hashtag = document.getString("hashtag");

            MongoCursor innerMongoCursor = lessthanh.find(new Document("hashtags.hashtag", hashtag)).iterator();
            innerMongoCursor.next();

            while (innerMongoCursor.hasNext()){
                Document innerDoc = (Document) innerMongoCursor.next();
                moveToCurr(lessthanh, curr, hashtag, innerDoc);
                deleteInner(lessthanh, innerDoc);
            }
        }
    }

    private void moveToHour(MongoCollection lessthanh, MongoCollection greaterthanh) {
        long tMinusOne = new Date().getTime() - 1000 * 60 * 60;
        Date date = new Date(tMinusOne);
        Document query = new Document().append("timestamp", new Document("$lte", date));
        MongoCursor mongoCursor = lessthanh.find(query).iterator();
        while(mongoCursor.hasNext()){
            Document curr = (Document) mongoCursor.next();
            List<Document> hashtags = (ArrayList) curr.get("hashtags");
            Date currDate = (Date) curr.get("timestamp");
            long lteHour = /*new Date().getTime()*/ currDate.getTime() - 1000*60*60;
            Date queryDate = new Date (lteHour);
            Document updateQuery = new Document().append("timestamp", new Document("$gte", queryDate));
            Document movedDoc = new Document().append("$push", new Document("hashtags", hashtags.get(0)));
            UpdateResult updateResult = greaterthanh.updateOne(updateQuery, movedDoc);
            if(updateResult.getMatchedCount()==0){
                Document newDoc = new Document();
                newDoc.append("timestamp", new Date()).append("hashtags", hashtags);
                greaterthanh.insertOne(newDoc);
            }
            deleteInner(lessthanh, curr);
        }

    }

    private void aggregateHashtags(MongoCollection greaterthanh) {
        MongoCursor outerMongoCursor = greaterthanh.find().iterator();
        while (outerMongoCursor.hasNext()){
            Document document = (Document) outerMongoCursor.next();
            List<Document> list = (ArrayList<Document>) document.get("hashtags");
            Set <String> hashtags = new HashSet();
            for (Document document1: list){
                hashtags.add((String) document1.get("hashtag"));
            }
            for(String hashtag: hashtags){
                int occurrences =0;
                for(Document document1:list){
                    if(document1.get("hashtag").equals(hashtag)){
                        occurrences = occurrences + document1.getInteger("occurrences");
                    }
                }
                ObjectId objectId = (ObjectId) document.get("_id");
                greaterthanh.updateOne(new Document("_id", objectId),new Document("$pull", new Document("hashtags",
                        new Document("hashtag", hashtag))));
                greaterthanh.updateOne(new Document("_id", objectId),new Document("$push", new Document("hashtags",
                        new Document("hashtag", hashtag).append("occurrences", occurrences))));
            }
        }
    }

    private void aggregateHashtags2(MongoCollection greaterthanh){
        MongoCursor outerMongoCursor = greaterthanh.find().iterator();
        while (outerMongoCursor.hasNext()){
            Document document = (Document) outerMongoCursor.next();
            List<Document> list = (ArrayList<Document>) document.get("hashtags");
            HashMap<String, Integer> hashtagHashMap = new HashMap<>();
            for (Document d: list){
                String hashtagString = d.getString("hashtag");
                int currOccurences = hashtagHashMap.get(hashtagString);
                hashtagHashMap.put(hashtagString, currOccurences + d.getInteger("occurrences"));
            }

            for (String hashtagString :hashtagHashMap.keySet()){
                ObjectId objectId = (ObjectId) document.get("_id");
                greaterthanh.updateOne(new Document("_id", objectId),new Document("$pull", new Document("hashtags",
                        new Document("hashtag", hashtagString))));
                greaterthanh.updateOne(new Document("_id", objectId),new Document("$push", new Document("hashtags",
                        new Document("hashtag", hashtagString).append("occurrences", hashtagHashMap.get(hashtagString)))));
            }

        }
    }


    private void moveToCurr(MongoCollection lessthanh, Document curr, String hashtag, Document innerDoc) {
        Date innerDate = (Date) innerDoc.get("timestamp");
        Date currDate = (Date) curr.get("timestamp");
        ObjectId currId = (ObjectId) curr.get("_id");
        String currMin = currDate.toString().substring(14, 16);
        String innerMin = innerDate.toString().substring(14, 16);
        if(innerMin.equals(currMin)){
            addToCurrentDoc(innerDoc, lessthanh, hashtag, currId);
        }
    }

    private void addToCurrentDoc(Document innerDoc, MongoCollection mongoCollection, String hashtag, ObjectId currId) {
        List <Document> newOccurrences = (ArrayList <Document>) innerDoc.get("hashtags");
        int occurrences = (int) newOccurrences.get(0).get("occurrences");
        Document query = new Document("hashtags.hashtag",hashtag).append("_id", currId);
        Document newDoc = new Document("$inc",new Document("hashtags.$.occurrences", occurrences));
        mongoCollection.updateOne(query, newDoc);
    }

    private void deleteInner(MongoCollection lessthanh, Document innerDoc) {
        ObjectId innerId = (ObjectId) innerDoc.get("_id");
        lessthanh.deleteOne(Document.parse("{_id: ObjectId(\'" +  innerId.toHexString() + "\')}"));
    }

    private MongoClient getMongoClient() {
        return this.mongoClient;
    }

    private MongoDatabase getDatabase() {
        return mongoClient.getDatabase("Hashtags");
    }

}
