package ru.sentinelcredit.service;

import lombok.extern.slf4j.Slf4j;
import ru.sentinelcredit.service.CampaignType;
import ru.sentinelcredit.service.ConfigType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

@Slf4j
public class StatType {

    private ConfigType configType = null;

    public StatType(ConfigType configType) {
        this.configType = configType;
    }

    public void sync (Connection conS, Connection conG) {
        Statement st = null;
        ResultSet rs = null;

        long startMillis = System.currentTimeMillis();

        try {
            st = conS.createStatement();
            st.setFetchSize(configType.getIFetchSize());
            rs = st.executeQuery("select s.row_id, s.x_gen_table_name from siebel.s_src s, siebel.s_src_x x where s.row_id = x.par_row_id and x.attrib_08 = 'Y' and x_gen_table_name is not null /*and s.row_id = '1-S7QWQW9'*/");
            while (rs.next()) {
                CampaignType campaignType = new CampaignType(rs.getString(1), rs.getString(2), configType);
                campaignType.prepareSync(conS);
                campaignType.sync(conS, conG);
                campaignType.sync2(conS, conG);
                campaignType.syncAPK(conS, conG);
                campaignType.commitSync(conS);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            try { rs.close(); } catch (Exception ignore) { }
            try { st.close(); } catch (Exception ignore) { }
        }

        long endMillis = System.currentTimeMillis();

        log.trace("All sync finished in {} sec", 1.0*(endMillis-startMillis)/1000);
    }
}
