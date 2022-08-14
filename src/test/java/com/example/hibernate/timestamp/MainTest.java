package com.example.hibernate.timestamp;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.jdbc.Work;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;

public class MainTest {

  @Test
  public void testMysqlSave() throws Exception {
    testSave("hibernate.mysql.cfg.xml");
  }

  @Test
  public void testMssqlSave() throws Exception {
    testSave("hibernate.mssql.cfg.xml");
  }

  @Test
  public void testPgSave() throws Exception {
    testSave("hibernate.pg.cfg.xml");
  }
  
  @Test
  public void testH2Save() throws Exception {
    testSave("hibernate.h2.cfg.xml");
  }

  private void testSave(String hibernateConfigFile) throws Exception {

    Configuration configuration = new Configuration();
    configuration.configure(hibernateConfigFile);
    configuration.addAnnotatedClass(Animal.class);

    TimeZone tz = TimeZone.getDefault();
    TimeZone auTimeZone = TimeZone.getTimeZone("Australia/Sydney");
    TimeZone bgTimeZone = TimeZone.getTimeZone("Europe/Belgrade");
    TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

    TimeZone.setDefault(auTimeZone);

    SessionFactory sessionFactory = configuration.buildSessionFactory();
    Session session = sessionFactory.openSession();

    Animal animal = new Animal();
    animal.setTitle("Mosquito");
    animal.setCreateTime(Instant.EPOCH.plus(0, ChronoUnit.HOURS));

    session.beginTransaction();
    session.save(animal);
    session.getTransaction().commit();
    
    session.evict(animal);
    
    // Load object in the same timezone, createTime should be equal
    Animal loadedAnimal = session.load(Animal.class, animal.getId());
    Assert.assertEquals(animal.getCreateTime(), loadedAnimal.getCreateTime());
    session.evict(loadedAnimal);
    
    session.doWork(new Work() {

      @Override
      public void execute(Connection connection) throws SQLException {
        String sql = "SELECT created_at FROM animal_test WHERE id = " + animal.getId();
        ResultSet rs = connection.createStatement().executeQuery(sql);
        if (rs.next()) {
          Timestamp ts = rs.getTimestamp(1);
          // For debug purposes to show how timestamp is loaded from the db via jdbc driver
          System.out.println("TIMESTAMP: " + ts);
        }
      }

    });

    TimeZone.setDefault(bgTimeZone);
    
    session.doWork(new Work() {

      @Override
      public void execute(Connection connection) throws SQLException {
        String sql = "SELECT created_at FROM animal_test WHERE id = " + animal.getId();
        ResultSet rs = connection.createStatement().executeQuery(sql);
        if (rs.next()) {
          Timestamp ts = rs.getTimestamp(1);
          // For debug purposes to show how timestamp is loaded from the db via jdbc driver in diff zone
          System.out.println("TIMESTAMP: " + ts);
        }
      }

    });
    
    // If zone is changes, dates should still be equal, but it appears it doesn't work with MSSQL and PGSQL jdbc drivers
    loadedAnimal = session.load(Animal.class, animal.getId());
    Assert.assertEquals(animal.getCreateTime(), loadedAnimal.getCreateTime());

    session.evict(loadedAnimal);

    // The same check if previous one passed, this one pass too
    TimeZone.setDefault(utcTimeZone);
    loadedAnimal = session.load(Animal.class, animal.getId());
    Assert.assertEquals(animal.getCreateTime(), loadedAnimal.getCreateTime());

    TimeZone.setDefault(tz);
  }
}
