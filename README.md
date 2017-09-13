# HashtagDataService
Data access layer for storing and accessing tuples of keywords and occurrences, along with timestamps, in a MongoDB database from a stream using jms messaging over ActiveMQ. I am building it alongside an application which collects and extracts hashtag data over the Twitter REST API using Apache Storm and Twitter4j, and routing to this application using Apache Camel.
