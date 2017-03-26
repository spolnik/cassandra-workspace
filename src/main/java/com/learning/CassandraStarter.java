package com.learning;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraStarter {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraStarter.class);

    public static void main(String[] args) {

        Cluster.Builder clusterBuilder = Cluster.builder().addContactPoint("127.0.0.1");

        try (Cluster cluster = clusterBuilder.build()) {
            Session session = cluster.connect();

            session.execute(
                    "INSERT INTO demo.users (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob');"
            );

            ResultSet resultSet = session.execute("SELECT * FROM demo.users where lastname='Jones'");

            resultSet.forEach(row -> LOG.info("{} {}", row.getString("firstname"), row.getInt("age")));

            // Delete the user from the users table
            session.execute("DELETE FROM demo.users WHERE lastname = 'Jones'");
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
}
