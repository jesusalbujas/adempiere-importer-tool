package org.ait.util;

import org.compiere.model.MTable;
import org.compiere.model.MColumn;
import org.compiere.util.DisplayType;
import org.compiere.util.Env;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class AIT_ColumnTypeResolver {

    /**
     * Returns the Java class associated with the column.
     */
    public static Class<?> getColumnType(String tableName, String columnName, String trxName) {
        MTable table = MTable.get(Env.getCtx(), tableName);
        if (table == null) {
            return String.class; // fallback
        }

        MColumn column = table.getColumn(columnName);
        if (column == null) {
            return String.class;
        }

        int displayType = column.getAD_Reference_ID();
        return mapDisplayType(displayType);
    }

    /**
     * Maps ADempiere DisplayType to Java class
     */
    private static Class<?> mapDisplayType(int displayType) {
        switch (displayType) {
            case DisplayType.ID:
            case DisplayType.Table:
            case DisplayType.TableDir:
                return Integer.class;

            case DisplayType.Amount:
            case DisplayType.Number:
            case DisplayType.CostPrice:
            case DisplayType.Quantity:
                return BigDecimal.class;

            case DisplayType.Date:
            case DisplayType.DateTime:
            case DisplayType.Time:
                return Timestamp.class;

            case DisplayType.YesNo:
                return Boolean.class;

            default:
                return String.class;
        }
    }

    /**
     * Converts a plain value into the correct data type for the column,
     * validating constraints from AD_Column (e.g. length).
     */
    public static Object castValue(String rawValue, String tableName, String columnName, String trxName) {
        if (rawValue == null || rawValue.isEmpty()) {
            return null;
        }

        // Get column metadata
        MTable table = MTable.get(Env.getCtx(), tableName);
        MColumn col = table != null ? table.getColumn(columnName) : null;

        int displayType = col != null ? col.getAD_Reference_ID() : DisplayType.String;

        try {
            // Length validation for text / list types
            if (DisplayType.isText(displayType) || displayType == DisplayType.List || displayType == DisplayType.String) {
                if (col != null) {
                    int maxLen = col.getFieldLength();
                    if (rawValue.length() > maxLen) {
                        throw new RuntimeException("Value too long: '" + rawValue +
                                "' for column " + tableName + "." + columnName +
                                " (max=" + maxLen + ", len=" + rawValue.length() + ")");
                    }
                }
                return rawValue;
            }

            // Numeric types
            if (DisplayType.isNumeric(displayType)) {
                return new BigDecimal(rawValue);
            }

            // Boolean types
            if (displayType == DisplayType.YesNo) {
                if ("Y".equalsIgnoreCase(rawValue) || "true".equalsIgnoreCase(rawValue) || "1".equals(rawValue)) {
                    return "Y";
                } else {
                    return "N";
                }
            }

            // --- Date types ---
            if (DisplayType.isDate(displayType)) {
                if (rawValue.length() == 10) {
                    return java.sql.Date.valueOf(rawValue); // yyyy-MM-dd
                } else {
                    return Timestamp.valueOf(rawValue); // yyyy-MM-dd HH:mm:ss
                }
            }

            // Fallback: text
            return rawValue;

        } catch (Exception e) {
            throw new RuntimeException(
                    "Error casting value '" + rawValue + "' for column " +
                    tableName + "." + columnName +
                    " (DisplayType=" + DisplayType.getDescription(displayType) + ")", e);
        }
    }
}
