package com.learning.driver;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSimpleStarter {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraSimpleStarter.class);

    public static void main(String[] args) {

        Cluster.Builder clusterBuilder = Cluster.builder().addContactPoint("127.0.0.1");

        try (Cluster cluster = clusterBuilder.build()) {
            Session session = cluster.connect();

            insertNewUser(session);

            ResultSet resultSet = queryUsers(session);

            resultSet.forEach(row ->
                    LOG.info("{} {}",
                            row.getString("firstname"),
                            row.getInt("age")
                    )
            );

            // Delete the user from the users table
            deleteUsers(session);
            // Show that the user is gone
            resultSet = session.execute("SELECT * FROM demo.users");
            resultSet.forEach(
                    row -> LOG.info(
                            "{} {} {} {} {}",
                            row.getString("lastname"),
                            row.getInt("age"),
                            row.getString("city"),
                            row.getString("email"),
                            row.getString("firstname")
                    )
            );
        }
    }

    private static void deleteUsers(Session session) {
        session.execute("DELETE FROM demo.users WHERE lastname = 'Jones'");
    }

    private static ResultSet queryUsers(Session session) {
        return session.execute(
                "SELECT * FROM demo.users where lastname='Jones'"
        );
    }

    private static void insertNewUser(Session session) {
        session.execute(
                "INSERT INTO demo.users (lastname, age, city, email, firstname) " +
                        "VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob');"
        );
    }
}
