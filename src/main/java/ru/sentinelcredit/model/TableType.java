package ru.sentinelcredit.model;

import lombok.Data;
import java.util.Date;

@Data
public class TableType {
    private Date ccUpdateId;
    String recordStatus;
    Integer cnt;
    Integer ord;
}
