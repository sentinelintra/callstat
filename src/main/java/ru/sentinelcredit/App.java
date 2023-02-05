package ru.sentinelcredit;

import lombok.extern.slf4j.Slf4j;
import ru.sentinelcredit.service.ConfigType;
import ru.sentinelcredit.service.StatType;

import java.sql.Connection;
import java.sql.DriverManager;

@Slf4j
public class App
{
    public static void main( String[] args )
    {
        ConfigType configType = new ConfigType(args);
        StatType statType = new StatType(configType);
        Connection conS = null;
        Connection conG = null;
        Boolean conSEnabled = true;
        Boolean conGEnabled = true;

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            while (true) {
                try {
                    conSEnabled = true;
                    if (conS == null || conS.isClosed()) {
                        conS = null;
                        conS = DriverManager.getConnection(configType.getUrlS(), configType.getUsernameS(), configType.getPasswordS());
                        conS.setAutoCommit(false);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                    conSEnabled = false;
                }

                try {
                    conGEnabled = true;
                    if (conG == null || conG.isClosed()) {
                        conG = null;
                        conG = DriverManager.getConnection(configType.getUrlG(), configType.getUsernameG(), configType.getPasswordG());
                        conG.setAutoCommit(false);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                    conGEnabled = false;
                }

                if (conSEnabled && conGEnabled) {
                    statType.sync(conS, conG);
                }

                Thread.sleep(60L * 1000L);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
