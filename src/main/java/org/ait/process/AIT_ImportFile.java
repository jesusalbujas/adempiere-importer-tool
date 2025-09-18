package org.ait.process;

import org.ait.model.X_AIT_ImportTemplate;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.ait.util.AIT_ColumnTypeResolver;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * AIT Import File process with advanced header parsing:
 * - header tokens can be:
 *      ColumnName
 *      ColumnName[LookupColumn]
 *      ColumnName/K   (K = unique key in the file)
 *      RelatedTable>ColumnName[LookupColumn]/K
 *
 * - If a token contains [LookupColumn], a lookup will be performed:
 *      SELECT <ColumnName> FROM <Table> WHERE <LookupColumn> = ?
 *   If ColumnName ends with "_ID", it is treated as returning an integer ID.
 *
 * - '/K' in a header (isKey) -> enforces that values in the file are unique 
 *   (an error is raised if duplicates are found).
 *
 * - The first header column is used as the WHERE clause in an UPDATE if 
 *   it has a value in the row; otherwise, a bulk UPDATE is performed by AD_Client_ID.
 *
 * - If a lookup does not find a match, or finds more than one row, 
 *   an Exception is thrown with details about the row/column.
 */
public class AIT_ImportFile extends AIT_ImportFileAbstract {

    private int templateId;
	private String tableName;

    @Override
    protected void prepare() {
        super.prepare();
        templateId = getRecord_ID();
    }

    @Override
    protected String doIt() throws Exception {
    	
    	
        if (templateId <= 0) {
            throw new Exception("No se especificó plantilla de importación");
        }

        X_AIT_ImportTemplate template = new X_AIT_ImportTemplate(getCtx(), templateId, get_TrxName());

        // Get table name from tab
        this.tableName = DB.getSQLValueString(get_TrxName(),
                "SELECT t.TableName FROM AD_Tab tab " +
                "JOIN AD_Table t ON t.AD_Table_ID=tab.AD_Table_ID " +
                "WHERE tab.AD_Tab_ID=?", template.getAD_Tab_ID());

        if (tableName == null) {
            throw new Exception("No se encontró tabla para la pestaña " + template.getAD_Tab_ID());
        }

        // Read UTF-8 file
        File file = new File(getPackageDir());
        if (!file.exists() || !file.isFile()) {
            throw new Exception("File not found: " + getPackageDir());
        }
        List<String> rawLines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
        // Remove lines that are empty (only spaces) to avoid confusion
        List<String> lines = new ArrayList<>();
        for (String L : rawLines) {
            if (L != null && !L.trim().isEmpty()) lines.add(L);
        }
        if (lines.isEmpty()) {
            throw new Exception("File is empty: " + getPackageDir());
        }

        // Detect separator in the first line of data (header)
        String headerLine = lines.get(0);
        String separator = ",";
        if (headerLine.contains(";")) separator = ";";
        else if (headerLine.contains("\t")) separator = "\t";

        // Headers: if the template has AIT_HeaderCSV, it is used (the template separator is a comma),
        // if not, it is parsed from the file with the detected separator.
        List<String> headerTokens;
        if (template.getAIT_HeaderCSV() != null && !template.getAIT_HeaderCSV().isEmpty()) {
            // The template uses comma as separator for the definition
            headerTokens = splitPreserveAll(template.getAIT_HeaderCSV(), ',');
        } else {
            headerTokens = splitPreserveAll(headerLine, separator.charAt(0));
        }


        // Parsing tokens to FieldSpec structures
        List<FieldSpec> fieldSpecs = new ArrayList<>();
        int colIndex = 0;
        for (String t : headerTokens) {
            colIndex++;
            FieldSpec fs = FieldSpec.parse(t.trim(), colIndex);
            fieldSpecs.add(fs);
        }

        // --- Prevalidation /K (uniqueness in file) ---
        // For each FieldSpec with isKey, ensure that it is not duplicated in the raw rows
        Map<FieldSpec, Map<String, Integer>> keyValueFirstRow = new HashMap<>();
        for (FieldSpec kfs : fieldSpecs) {
            if (!kfs.isKey) continue;
            keyValueFirstRow.put(kfs, new HashMap<>());
        }

        // Parsing raw rows into memory before executing actions to be able to validate duplicates
        List<Map<FieldSpec, String>> rawRows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] values = line.split(Pattern.quote(separator), -1);
            // Normalizing (trimming) and ensuring quantity of columns
            if (values.length < fieldSpecs.size()) {
                throw new Exception("Row " + (i + 1) + " incomplete. Expected: " + fieldSpecs.size()
                        + ", found: " + values.length + "  -> Line: " + line);
            }
            Map<FieldSpec, String> rowMap = new LinkedHashMap<>();
            for (int c = 0; c < fieldSpecs.size(); c++) {
                String raw = values[c] == null ? "" : values[c].trim();
                rowMap.put(fieldSpecs.get(c), raw);
                // check keys
                FieldSpec f = fieldSpecs.get(c);
                if (f.isKey) {
                    Map<String, Integer> seen = keyValueFirstRow.get(f);
                    if (seen.containsKey(raw)) {
                        int firstRow = seen.get(raw);
                        throw new Exception("Duplicate key value in column '" + f.original +
                                "' (cell row " + (i + 1) + "). Value: '" + raw +
                                "'. Already appears in row " + firstRow + ".");
                    } else {
                        seen.put(raw, i + 0); // store row index (1-based but i starts at 1)
                    }
                }
            }
            rawRows.add(rowMap);
        }

        int inserted = 0;
        int updated = 0;

        // Processing row by row (resolving lookups and executing SQL)
        int fileRowNumber = 1; // header is row 1, data starts at 2 logically; but our rawRows index 0 corresponds to file row 2
        for (int r = 0; r < rawRows.size(); r++) {
            fileRowNumber = r + 2; // human readable: header is 1, first data row is 2
            Map<FieldSpec, String> rawRow = rawRows.get(r);

            // First we resolve values ​​for columns in the main table (map: columnName -> ResolvedValue)
            // Resolution takes into account lookups defined by [LookupCol] or RelatedTable>Column...
            Map<String, Object> resolvedColumns = new LinkedHashMap<>();
            for (FieldSpec fs : fieldSpecs) {
                String rawValue = rawRow.get(fs);
                Object resolved = resolveFieldValue(fs, rawValue, fileRowNumber);
                // For columns that represent "direct" (without '>') we assign the target column name,
                // for path we still use the last target token as the column name to set.
                String dbCol = fs.targetColumn; // example: AD_Org_ID, Name, pst_employees_id, etc.
                resolvedColumns.put(dbCol, resolved);
            }

            // update
            if ("U".equalsIgnoreCase(getOptions())) {
                List<FieldSpec> keyFields = new ArrayList<>();
                for (FieldSpec fs : fieldSpecs) {
                    if (fs.isKey) keyFields.add(fs);
                }

                if (keyFields.isEmpty()) {
                    int adClient = firstNonZero(template.getAD_Client_ID(), Env.getAD_Client_ID(getCtx()));
                    Map<String, Object> sets = new LinkedHashMap<>(resolvedColumns);
                    String sql = buildUpdateSql(tableName, sets.keySet(), Collections.singletonList("AD_Client_ID"));
                    try (PreparedStatement pstmt = DB.prepareStatement(sql, get_TrxName())) {
                        int idx = 1;
                        for (Object v : sets.values()) {
                            pstmt.setObject(idx++, v);
                        }
                        pstmt.setObject(idx, adClient);
                        int rows = pstmt.executeUpdate();
                        updated += rows;
                    }
                } else {
                    Map<String, Object> sets = new LinkedHashMap<>(resolvedColumns);
                    List<String> whereCols = new ArrayList<>();
                    List<Object> whereValues = new ArrayList<>();
                    for (FieldSpec kfs : keyFields) {
                        String rawValue = rawRow.get(kfs);
                        Object whereVal = resolveFieldValue(kfs, rawValue, fileRowNumber);
                        whereCols.add(kfs.targetColumn);
                        whereValues.add(whereVal);
                        sets.remove(kfs.targetColumn);
                    }

                    if (sets.isEmpty()) continue;

                    String sql = buildUpdateSql(tableName, sets.keySet(), whereCols);
                    try (PreparedStatement pstmt = DB.prepareStatement(sql, get_TrxName())) {
                        int idx = 1;
                        for (Object v : sets.values()) {
                            pstmt.setObject(idx++, v);
                        }
                        for (Object wv : whereValues) {
                            pstmt.setObject(idx++, wv);
                        }
                        int rows = pstmt.executeUpdate();
                        updated += rows;
                        if (rows == 0) {
                            throw new Exception("Row " + fileRowNumber +
                                    ": UPDATE did not find a record with keys " + whereCols + "=" + whereValues);
                        }
                    }
                }
            } else {
                // INSERT
                // Add required columns if they are not included in resolvedColumns
                Map<String, Object> allCols = addSystemColumns(tableName, resolvedColumns, template);

                // System.out.println("Row " + fileRowNumber + " => " + resolvedColumns);

                // Building dynamic INSERT
                String sql = "INSERT INTO " + tableName + " (" +
                        String.join(",", allCols.keySet()) + ") VALUES (" +
                        String.join(",", Collections.nCopies(allCols.size(), "?")) + ")";

                try (PreparedStatement pstmt = DB.prepareStatement(sql, get_TrxName())) {
                    int idx = 1;
                    for (Object v : allCols.values()) {
                        pstmt.setObject(idx++, v);
                    }
                    inserted += pstmt.executeUpdate();
                }
            }
        } // end rows

        return "Import finished. Inserted=" + inserted + ", Updated=" + updated;
    }

    // --------------------
    // Helper & parsing
    // --------------------

    /** Splits a string preserving all columns (including empty ones). Simple char separator. */
    private static List<String> splitPreserveAll(String s, char sep) {
        List<String> out = new ArrayList<>();
        if (s == null) return out;
        String[] arr = s.split(Pattern.quote(String.valueOf(sep)), -1);
        for (String a : arr) out.add(a);
        return out;
    }

    /** Builds dynamic SQL UPDATE with given WHERE columns */
    private static String buildUpdateSql(String tableName, Set<String> setCols, List<String> whereCols) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(tableName).append(" SET ");
        boolean first = true;
        for (String c : setCols) {
            if (!first) sb.append(",");
            sb.append(c).append("=?");
            first = false;
        }
        sb.append(" WHERE ");
        for (int i = 0; i < whereCols.size(); i++) {
            if (i > 0) sb.append(" AND ");
            sb.append(whereCols.get(i)).append("=?");
        }
        return sb.toString();
    }

    /** Resolves the final value to be used for the column represented by fs with the given rawValue.
     *  If fs has a lookup (fs.lookupColumn != null), a query will be made to the corresponding table.
     *  Throws Exception with a clear message about row/column if it fails.
     */
    /** Resolves the final value to be used for the column represented by fs with the given rawValue. */
    private Object resolveFieldValue(FieldSpec fs, String rawValue, int fileRowNumber) throws Exception {
        if (rawValue == null || rawValue.isEmpty() || "(null)".equalsIgnoreCase(rawValue)) {
            return null;
        }

        // Actual column type
        Class<?> type = AIT_ColumnTypeResolver.getColumnType(this.tableName, fs.targetColumn, get_TrxName());

        // If it's numeric and rawValue is a number → use directly
        if ((type == Integer.class || type == BigDecimal.class) && rawValue.matches("\\d+")) {
            return type == Integer.class ? Integer.parseInt(rawValue) : new BigDecimal(rawValue);
        }

        // If it has a lookup -> search
        if (fs.lookupColumn != null) {
            String currentTable;
            String targetColumn = fs.targetColumn;

            if (fs.pathParts.length == 1) {
                currentTable = targetColumn.substring(0, targetColumn.length() - 3); // ej: C_BPartner_ID → C_BPartner
            } else {
                currentTable = fs.pathParts[fs.pathParts.length - 2];
            }

            String sql = "SELECT " + targetColumn + " FROM " + currentTable + " WHERE " + fs.lookupColumn + "=?";
            Object dbValue = DB.getSQLValue(get_TrxName(), sql, rawValue); // returns Integer/BigDecimal depending on column

                        if (dbValue == null) {
                            throw new Exception("Row " + fileRowNumber + ", column " + fs.columnIndex +
                                    " (" + fs.original + "): Value '" + rawValue +
                                    "' not found in " + currentTable + "." + fs.lookupColumn);
                        }

            return dbValue;
        }

        // Direct case (text or numeric)
        return AIT_ColumnTypeResolver.castValue(rawValue, this.tableName, fs.targetColumn, get_TrxName());
    }

    // FieldSpec represents a header column with its metadata (lookup, key, path)
    private static class FieldSpec {
        final String original;        // token tal cual en header
        final String[] pathParts;     // if contains '>', split by '>' (e.g. AD_User, C_BPartner_ID)
        final String targetColumn;    // last part (e.g. C_BPartner_ID or Name)
        final String lookupColumn;    // if has [LookupColumn], e.g. Name or Value
        final boolean isKey;          // /K
        final int columnIndex;        // position in header (1-based)

        private static final Pattern BRACKETS = Pattern.compile("(.+?)\\[(.+?)\\]");

        private FieldSpec(String original, String[] pathParts, String lookupColumn, boolean isKey, int columnIndex) {
            this.original = original;
            this.pathParts = pathParts;
            this.lookupColumn = lookupColumn;
            this.isKey = isKey;
            this.columnIndex = columnIndex;
            this.targetColumn = pathParts[pathParts.length - 1].trim();
        }

        static FieldSpec parse(String token, int columnIndex) {
            String orig = token;
            boolean isKey = false;
            // detect /K at end
            if (orig.endsWith("/K") || orig.endsWith("/k")) {
                isKey = true;
                orig = orig.substring(0, orig.length() - 2).trim();
            }
            String lookup = null;
            Matcher m = BRACKETS.matcher(orig);
            if (m.find()) {
                orig = m.group(1).trim();
                lookup = m.group(2).trim();
            }
            // split by '>'
            String[] parts = Arrays.stream(orig.split(">"))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new);
            return new FieldSpec(token, parts, lookup, isKey, columnIndex);
        }
    }

    // --------------------
    // Small/helpers utilities
    // --------------------

    private int parseIntSafe(String val) {
        if (val == null) return 0;
        try {
            return Integer.parseInt(val);
        } catch (Exception e) {
            return 0;
        }
    }

    private int firstNonZero(int... values) {
        if (values == null) return 0;
        for (int v : values) {
            if (v > 0) return v;
        }
        return 0;
    }

    @SafeVarargs
    private final <T> T firstNonNull(T... values) {
        if (values == null) return null;
        for (T v : values) if (v != null) return v;
        return null;
    }

    /** Adds mandatory ADempiere system columns to a record before INSERT */
    private Map<String, Object> addSystemColumns(String tableName, Map<String, Object> resolvedColumns, X_AIT_ImportTemplate template) {
        Map<String, Object> allCols = new LinkedHashMap<>(resolvedColumns);

        // Dynamic Primary Key: <TableName>_ID
        String pkCol = tableName + "_ID";
        if (!allCols.containsKey(pkCol)) {
            
            int adTableId = DB.getSQLValue(
            get_TrxName(),
            "SELECT AD_Table_ID FROM AD_Table WHERE TableName=?", tableName
            );
            if (adTableId <= 0) {
                throw new IllegalArgumentException("No se encontró AD_Table para " + tableName);
            }

            int nextId = DB.getSQLValue(get_TrxName(),
            	    "SELECT nextid(" + adTableId + ", 'N')");
            
            allCols.put(pkCol, nextId);
        }

        // Client & Org
        allCols.putIfAbsent("AD_Client_ID",
                firstNonZero(template.getAD_Client_ID(),
                        parseIntSafe(String.valueOf(allCols.get("AD_Client_ID"))),
                        Env.getAD_Client_ID(getCtx())));
        allCols.putIfAbsent("AD_Org_ID",
                firstNonZero(template.getAD_Org_ID(),
                        parseIntSafe(String.valueOf(allCols.get("AD_Org_ID"))),
                        Env.getAD_Org_ID(getCtx())));
        // If the table has IsActive column, and it's not in the file, set 'Y'
        Integer isActiveCol = DB.getSQLValue(
                get_TrxName(),
                "SELECT COUNT(*) FROM AD_Column WHERE AD_Table_ID=(SELECT AD_Table_ID FROM AD_Table WHERE TableName=?) AND ColumnName='IsActive'",
                tableName);
        if (isActiveCol != null && isActiveCol > 0) {
            allCols.putIfAbsent("IsActive", "Y");
        }

        // User and timestamps
        int userId = Env.getAD_User_ID(getCtx());
        Timestamp now = new Timestamp(System.currentTimeMillis());

        allCols.putIfAbsent("Created", now);
        allCols.putIfAbsent("CreatedBy", userId);
        allCols.putIfAbsent("Updated", now);
        allCols.putIfAbsent("UpdatedBy", userId);

        // UUID
        allCols.putIfAbsent("UUID", UUID.randomUUID().toString());

        return allCols;
    }

}
