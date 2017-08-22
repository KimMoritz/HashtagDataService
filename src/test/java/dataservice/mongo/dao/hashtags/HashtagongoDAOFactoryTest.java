package dataservice.mongo.dao.hashtags;

import org.junit.Test;

import static org.junit.Assert.*;

public class HashtagongoDAOFactoryTest {
    @Test
    public void mongoDAOShouldReturnMongoDAO() throws Exception {
        assertEquals("Should return a HashtagMongoDAO",
                HashtagMongoDAO.class, HashtagongoDAOFactory.mongoDAO().getClass());
        assertNotNull(HashtagongoDAOFactory.mongoDAO().getClass());
    }

}