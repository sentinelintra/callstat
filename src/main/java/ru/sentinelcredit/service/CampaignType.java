package ru.sentinelcredit.service;

import lombok.extern.slf4j.Slf4j;
import ru.sentinelcredit.model.TableType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

@Slf4j
public class CampaignType {
    private String rowId = null;
    private String tableName = null;
    private ConfigType configType = null;
    private SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String[] recordStatus = { "[No Record Status]", "Ready", "Retrieved", "Updated", "Stale", "Cancelled", "Agent Error", "Chain Updated",
            "Missed CallBack", "Chain Ready", "Delegated" };

    public CampaignType (String rowId, String tableName, ConfigType configType) {
        this.rowId = rowId;
        this.tableName = tableName;
        this.configType = configType;
    }

    public void prepareSync(Connection conS) {
        PreparedStatement pst = null;

        try {
            pst = conS.prepareStatement("delete from siebel.cx_campaign_stt where campaign_id = ? and type = 'CL_STATS1'");
            pst.setString(1, rowId);
            pst.execute();
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            try { pst.close(); } catch (Exception ignore) { }
        }
    }

    public void commitSync(Connection conS) {
        try {
            conS.commit();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public void sync (Connection conS, Connection conG) {
        PreparedStatement pst = null;
        Statement st = null;
        ResultSet rs = null;
        Set<TableType> tableTypes = new HashSet();

        try {
            st = conG.createStatement();
            st.setFetchSize(configType.getIFetchSize());
            rs = st.executeQuery("select x_cc_upd_id, record_status, count(1) from genesyssql." + tableName + " group by x_cc_upd_id, record_status");
            while (rs.next()) {
                TableType tableType = new TableType();
                tableType.setCcUpdateId(rs.getDate(1));
                tableType.setRecordStatus((rs.getInt(2) > 10) ? "?": recordStatus[rs.getInt(2)]);
                tableType.setCnt(rs.getInt(3));
                tableTypes.add(tableType);
            }
        } catch (Exception e) {
            log.error("sync RowId => {} TableName => {} Message=", rowId, tableName, e);
        } finally {
            try { st.close(); } catch (Exception ignore) { }
            try { rs.close(); } catch (Exception ignore) { }
        }

        try {
            if (tableTypes.size() > 0) {

                pst = conS.prepareStatement("insert into siebel.cx_campaign_stt ( row_id, created_by, last_upd_by, campaign_id, comments, name, type, val1, ord )" +
                        " values ( s_sequence_pkg.get_next_rowid, '0-1', '0-1', ?, ?, ?, 'CL_STATS1', ?, ? )");

                Integer n = 50;
                Iterator<TableType> i = tableTypes.iterator();
                while (i.hasNext()) {
                    TableType tt = i.next();
                    pst.setString(1, rowId);
                    pst.setString(2, tt.getRecordStatus());
                    pst.setString(3, (tt.getCcUpdateId() == null) ? null: format1.format(tt.getCcUpdateId()));
                    pst.setInt(4, tt.getCnt());
                    pst.setInt(5, n);
                    pst.addBatch();
                    n++;
                }

                pst.executeBatch();
            }
        } catch (Exception e) {
            log.error("sync RowId => {} TableName => {} Message=", rowId, tableName, e);
        } finally {
            try { pst.close(); } catch (Exception ignore) { }
        }
    }

    public void sync2 (Connection conS, Connection conG) {
        PreparedStatement pst = null;
        Statement st = null;
        ResultSet rs = null;
        Set<TableType> tableTypes = new HashSet();
        Integer filterCount = configType.getIntegerProperty("filter_count");

        for (int i = 1; i <= filterCount; i++) {
            String filter = configType.getStringProperty("filter_"+i);
            String filterName = configType.getStringProperty("filter_name_"+i);

            try {
                st = conG.createStatement();
                st.setFetchSize(1);
                rs = st.executeQuery("select count(1) from genesyssql." + tableName + " where "+filter);
                if (rs.next()) {
                    TableType tt = new TableType();
                    tt.setRecordStatus(filterName);
                    tt.setCnt(rs.getInt(1));
                    tt.setOrd(i);
                    tableTypes.add(tt);
                }
            } catch (Exception e) {
                log.error("sync2 RowId => {} TableName => {} Message=", rowId, tableName, e);
            } finally {
                try { st.close(); } catch (Exception ignore) { }
                try { rs.close(); } catch (Exception ignore) { }
            }
        }

        try {
            if (tableTypes.size() > 0) {

                pst = conS.prepareStatement("insert into siebel.cx_campaign_stt ( row_id, created_by, last_upd_by, campaign_id, comments, name, type, val1, ord )" +
                        " values ( s_sequence_pkg.get_next_rowid, '0-1', '0-1', ?, ?, '---------------------------', 'CL_STATS1', ?, ? )");

                Iterator<TableType> i = tableTypes.iterator();
                while (i.hasNext()) {
                    TableType tt = i.next();
                    pst.setString(1, rowId);
                    pst.setString(2, tt.getRecordStatus());
                    pst.setInt(3, tt.getCnt());
                    pst.setInt(4, tt.getOrd());
                    pst.addBatch();
                }

                pst.executeBatch();
            }
        } catch (Exception e) {
            log.error("sync2 RowId => {} TableName => {} Message=", rowId, tableName, e);
        } finally {
            try { pst.close(); } catch (Exception ignore) { }
        }
    }

    public void syncAPK (Connection conS, Connection conG) {
        PreparedStatement pst = null;
        Statement st = null;
        ResultSet rs = null;
        Float all = 0F, mob = 0F, tgt;

        try {
            st = conG.createStatement();
            st.setFetchSize(1);
            rs = st.executeQuery("select count(1) from genesyssql." + tableName + " where call_result in (21,28,52,6) and contact_info_type2 !=0 and portfolio_name is null");
            if (rs.next())
                all = rs.getFloat(1);
        } catch (Exception e) {
            log.error("syncAPK RowId => {} TableName => {} Message=", rowId, tableName, e);
        } finally {
            try { st.close(); } catch (Exception ignore) { }
            try { rs.close(); } catch (Exception ignore) { }
        }

        try {
            st = conG.createStatement();
            st.setFetchSize(1);
            rs = st.executeQuery("select count(1) from genesyssql." + tableName + " where " +
                    " contact_info_type2 in (4,6) /*только МОБ*/" +
                    " and portfolio_name is null " +
                    " and call_result in (21,28,52,6)  /*только готовые к обзвону*/ " +
                    " and record_status in (1,2) /*только готовые к обзвону*/" +
                    " and (dial_sched_time < ((sysdate-to_date('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:Mi:SS'))*86400 + to_number(0)*60*60) or dial_sched_time is null) /*Время перезвона или наступило или не задано*/" +
                    " and ((sysdate-trunc(sysdate)-3/24)*24 + to_number(nvl(tz_dbid2,'3')))*60*60 between daily_from and daily_till /*Местное время должника в допустимом интервале От и До*/");
            if (rs.next())
                mob = rs.getFloat(1);
        } catch (Exception e) {
            log.error("syncAPK RowId => {} TableName => {} Message=", rowId, tableName, e);
        } finally {
            try { st.close(); } catch (Exception ignore) { }
            try { rs.close(); } catch (Exception ignore) { }
        }

        try {
            pst = conS.prepareStatement("update siebel.s_src set tgt_calls_per_day = ? where row_id = ?");
            pst.setFloat(1, 100F-((all == 0F) ? 0F : mob/all*100));
            pst.setString(2, rowId);
            pst.execute();
        } catch (Exception e) {
            log.error("syncAPK RowId => {} TableName => {} Message=", rowId, tableName, e);
        } finally {
            try { pst.close(); } catch (Exception ignore) { }
        }
    }
}
