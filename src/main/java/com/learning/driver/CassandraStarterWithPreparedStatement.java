package com.learning.driver;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraStarterWithPreparedStatement {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraStarterWithPreparedStatement.class);

    public static void main(String[] args) {

        Cluster.Builder clusterBuilder = Cluster.builder().addContactPoint("127.0.0.1");

        try (Cluster cluster = clusterBuilder.build()) {
            Session session = cluster.connect();

            PreparedStatement insertStatement = session.prepare(
                    "INSERT INTO demo.users" + "(lastname, age, city, email, firstname)"
                            + "VALUES (?,?,?,?,?);"
            );

            BoundStatement boundStatement = new BoundStatement(insertStatement);

            session.execute(boundStatement.bind("Spólnik", 30, "Kraków", "jacek@example.com", "Jacek"));
            displayAll(session);

            Statement update = QueryBuilder.update("demo", "users")
                    .with(QueryBuilder.set("age", 31))
                    .where((QueryBuilder.eq("lastname", "Spólnik")));

            session.execute(update);
            displayAll(session);

            Statement delete = QueryBuilder.delete().from("demo", "users")
                    .where(QueryBuilder.eq("lastname", "Spólnik"));

            session.execute(delete);
            displayAll(session);
        }
    }

    private static void displayAll(Session session) {
        Statement select = QueryBuilder.select().all().from("demo", "users");

        session.execute(select).forEach(
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
