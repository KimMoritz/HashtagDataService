package dataservice;

import dataservice.HashtagFetcher;
import dataservice.mongo.dao.MongoClientHandler;
import dataservice.mongo.dao.MongoDAO;
import dataservice.mongo.dao.hashtags.HashtagongoDAOFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Timer;
import java.util.TimerTask;

@Component
public class MongoMaintainer {
    private MongoDAO mongoDAO;
    private Timer maintenenceTimer;

    public MongoMaintainer() {
        this.mongoDAO = HashtagongoDAOFactory.mongoDAO();
        this.maintenenceTimer = new Timer();
        this.maintenenceTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("Performing maintenance at " + LocalDateTime.now());
                mongoDAO.performMaintenance(MongoClientHandler.getMongoClient());
            }
        }, 0, 10*60*1000);
    }

}