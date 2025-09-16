package org.ait.process;

import org.ait.model.X_AIT_ImportTemplate;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Env;

import java.io.File;
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
 *      ColumnName/K   (K = unique key in file)
 *      RelatedTable>ColumnName[LookupColumn]/K
 *
 * - If a token contains [LookupColumn] we will perform a lookup:
 *      SELECT <ColumnName> FROM <Table> WHERE <LookupColumn> = ?
 *   If ColumnName ends with "_ID" we treat it as returning an integer id.
 *
 * - '/K' en un header (isKey) -> obliga que los valores en el archivo sean únicos (error si se repiten)
 *
 * - Primera columna del header se usa como WHERE en UPDATE si tiene valor en la fila; si no, UPDATE masivo por AD_Client_ID.
 *
 * - Si un lookup no encuentra nada, o encuentra >1 fila, se lanza Exception con detalle de fila/columna.
 */
public class AIT_ImportFile extends AIT_ImportFileAbstract {

    private int templateId;

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

        // Obtener nombre de la tabla desde la pestaña
        String tableName = DB.getSQLValueString(get_TrxName(),
                "SELECT t.TableName FROM AD_Tab tab " +
                        "JOIN AD_Table t ON t.AD_Table_ID=tab.AD_Table_ID " +
                        "WHERE tab.AD_Tab_ID=?", template.getAD_Tab_ID());

        if (tableName == null) {
            throw new Exception("No se encontró tabla para la pestaña " + template.getAD_Tab_ID());
        }

        // Leer archivo UTF-8
        File file = new File(getPackageDir());
        if (!file.exists() || !file.isFile()) {
            throw new Exception("Archivo no encontrado: " + getPackageDir());
        }
        List<String> rawLines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
        // Eliminar líneas que estén vacías (solo espacios) para evitar confusión
        List<String> lines = new ArrayList<>();
        for (String L : rawLines) {
            if (L != null && !L.trim().isEmpty()) lines.add(L);
        }
        if (lines.isEmpty()) {
            throw new Exception("Archivo vacío");
        }

        // Detectar separador en la primera línea de datos (header)
        String headerLine = lines.get(0);
        String separator = ",";
        if (headerLine.contains(";")) separator = ";";
        else if (headerLine.contains("\t")) separator = "\t";

        // Cabeceras: si plantilla tiene AIT_HeaderCSV se usa (separador en plantilla es coma),
        // si no, se parsea desde archivo con el separador detectado.
        List<String> headerTokens;
        if (template.getAIT_HeaderCSV() != null && !template.getAIT_HeaderCSV().isEmpty()) {
            // la plantilla usa coma como separador para la definición
            headerTokens = splitPreserveAll(template.getAIT_HeaderCSV(), ',');
        } else {
            headerTokens = splitPreserveAll(headerLine, separator.charAt(0));
        }

        // Parsear tokens a estructuras FieldSpec
        List<FieldSpec> fieldSpecs = new ArrayList<>();
        int colIndex = 0;
        for (String t : headerTokens) {
            colIndex++;
            FieldSpec fs = FieldSpec.parse(t.trim(), colIndex);
            fieldSpecs.add(fs);
        }

        // --- Prevalidación /K (unicidad en archivo) ---
        // Para cada FieldSpec con isKey, asegurar que no exista repetido en las filas raw
        Map<FieldSpec, Map<String, Integer>> keyValueFirstRow = new HashMap<>();
        for (FieldSpec kfs : fieldSpecs) {
            if (!kfs.isKey) continue;
            keyValueFirstRow.put(kfs, new HashMap<>());
        }

        // Parseear filas crudas en memoria antes de ejecutar acciones para poder validar duplicados
        List<Map<FieldSpec, String>> rawRows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] values = line.split(Pattern.quote(separator), -1);
            // Normalizar (trim) y asegurar cantidad
            if (values.length < fieldSpecs.size()) {
                throw new Exception("Fila " + (i + 1) + " incompleta. Esperadas: " + fieldSpecs.size()
                        + ", encontradas: " + values.length + "  -> Línea: " + line);
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
                        throw new Exception("Valor repetido de clave en columna '" + f.original +
                                "' (celda fila " + (i + 1) + "). Valor: '" + raw +
                                "'. Ya aparece en fila " + firstRow + ".");
                    } else {
                        seen.put(raw, i + 0); // store row index (1-based but i starts at 1)
                    }
                }
            }
            rawRows.add(rowMap);
        }

        int inserted = 0;
        int updated = 0;

        // Ahora procesar fila por fila (resolviendo lookups y ejecutando SQL)
        int fileRowNumber = 1; // header is row 1, data starts at 2 logically; but our rawRows index 0 corresponds to file row 2
        for (int r = 0; r < rawRows.size(); r++) {
            fileRowNumber = r + 2; // human readable: header is 1, first data row is 2
            Map<FieldSpec, String> rawRow = rawRows.get(r);

            // Primero resolvemos valores para columnas del main table (map: columnName -> resolvedValue)
            // Resolución tiene en cuenta lookups definidos por [LookupCol] o RelatedTable>Column...
            Map<String, Object> resolvedColumns = new LinkedHashMap<>();
            for (FieldSpec fs : fieldSpecs) {
                String rawValue = rawRow.get(fs);
                Object resolved = resolveFieldValue(fs, rawValue, fileRowNumber);
                // Para columnas que representan "directo" (sin '>') asignamos el target column name,
                // para path we still use the last target token as the column name to set.
                String dbCol = fs.targetColumn; // ejemplo: AD_Org_ID, Name, pst_employees_id, etc.
                resolvedColumns.put(dbCol, resolved);
            }

            // Ahora ejecutar Insert o Update
            if ("U".equalsIgnoreCase(getOptions())) {
                // UPDATE: si la primera columna del header tiene valor -> WHERE con esa columna,
                // si está vacía -> UPDATE masivo por AD_Client_ID
                FieldSpec firstFs = fieldSpecs.get(0);
                String rawWhereValue = rawRow.get(firstFs);
                Object whereResolved = resolveFieldValue(firstFs, rawWhereValue, fileRowNumber);
                boolean hasWhere = (whereResolved != null) && (!String.valueOf(whereResolved).isEmpty());

                if (hasWhere) {
                    // construir update usando columns excepto la columna de where
                    String whereCol = firstFs.targetColumn;
                    Map<String, Object> sets = new LinkedHashMap<>(resolvedColumns);
                    sets.remove(whereCol);

                    if (sets.isEmpty()) {
                        // nada que actualizar
                        continue;
                    }

                    String sql = buildUpdateSql(tableName, sets.keySet(), whereCol);
                    try (PreparedStatement pstmt = DB.prepareStatement(sql, get_TrxName())) {
                        int idx = 1;
                        for (Object v : sets.values()) {
                            pstmt.setObject(idx++, v);
                        }
                        // where value (posible entero o string)
                        pstmt.setObject(idx, whereResolved);
                        int rows = pstmt.executeUpdate();
                        updated += rows;
                        if (rows == 0) {
                            // No se actualizó: eso puede indicar que el where no encontró el registro
                            throw new Exception("Fila " + fileRowNumber + ": UPDATE no encontró registro con " +
                                    whereCol + "=" + whereResolved + " (header: " + firstFs.original + ")");
                        }
                    }
                } else {
                    // UPDATE masivo por AD_Client_ID (usar resolved value del template o contexto)
                    int adClient = firstNonZero(template.getAD_Client_ID(), Env.getAD_Client_ID(getCtx()));
                    Map<String, Object> sets = new LinkedHashMap<>(resolvedColumns);
                    // No se debe incluir AD_Client_ID en SET si existe; opcional, pero lo dejamos
                    String sql = buildUpdateSql(tableName, sets.keySet(), "AD_Client_ID");
                    try (PreparedStatement pstmt = DB.prepareStatement(sql, get_TrxName())) {
                        int idx = 1;
                        for (Object v : sets.values()) {
                            pstmt.setObject(idx++, v);
                        }
                        pstmt.setObject(idx, adClient);
                        int rows = pstmt.executeUpdate();
                        updated += rows;
                    }
                }
            } else {
                // INSERT
                // añadimos columnas obligatorias si no vienen en resolvedColumns
                Map<String, Object> allCols = new LinkedHashMap<>(resolvedColumns);

                // PK nextid
                int nextId = DB.getSQLValue(get_TrxName(),
                        "SELECT nextid(" + DB.TO_STRING(tableName) + ", 'N')");
                allCols.putIfAbsent(tableName + "_ID", nextId);

                // AD_Client_ID, AD_Org_ID prefer template, then CSV, then context
                allCols.putIfAbsent("AD_Client_ID",
                        firstNonZero(template.getAD_Client_ID(),
                                parseIntSafe(String.valueOf(allCols.get("AD_Client_ID"))),
                                Env.getAD_Client_ID(getCtx())));
                allCols.putIfAbsent("AD_Org_ID",
                        firstNonZero(template.getAD_Org_ID(),
                                parseIntSafe(String.valueOf(allCols.get("AD_Org_ID"))),
                                Env.getAD_Org_ID(getCtx())));

                int userId = Env.getAD_User_ID(getCtx());
                Timestamp now = new Timestamp(System.currentTimeMillis());

                allCols.putIfAbsent("Created", firstNonNull(template.getCreated(), now));
                allCols.putIfAbsent("CreatedBy", firstNonZero(template.getCreatedBy(), userId));
                allCols.putIfAbsent("Updated", firstNonNull(template.getUpdated(), now));
                allCols.putIfAbsent("UpdatedBy", firstNonZero(template.getUpdatedBy(), userId));
                allCols.putIfAbsent("UUID", firstNonNull(template.getUUID(), UUID.randomUUID().toString()));

                // Construir INSERT
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
        } // end filas

        return "Importación finalizada. Insertados=" + inserted + ", Actualizados=" + updated;
    }

    // --------------------
    // Helper & parsing
    // --------------------

    /** Divide respetando todas las columnas (incluso vacías). char sep simple. */
    private static List<String> splitPreserveAll(String s, char sep) {
        List<String> out = new ArrayList<>();
        if (s == null) return out;
        String[] arr = s.split(Pattern.quote(String.valueOf(sep)), -1);
        for (String a : arr) out.add(a);
        return out;
    }

    /** Construye SQL UPDATE dinámico con columna WHERE dada */
    private static String buildUpdateSql(String tableName, Set<String> setCols, String whereCol) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(tableName).append(" SET ");
        boolean first = true;
        for (String c : setCols) {
            if (!first) sb.append(",");
            sb.append(c).append("=?");
            first = false;
        }
        sb.append(" WHERE ").append(whereCol).append("=?");
        return sb.toString();
    }

    /** Resuelve el valor final a ser usado para la columna representada por fs con valor rawValue.
     *  Si fs tiene lookup (fs.lookupColumn != null) se hará una consulta a la tabla correspondiente.
     *  Lanza Exception con mensaje claro de fila/columna si falla.
     */
    private Object resolveFieldValue(FieldSpec fs, String rawValue, int fileRowNumber) throws Exception {
        // (null) explícito
        if ("(null)".equalsIgnoreCase(rawValue)) {
            return null;
        }
        if (rawValue == null || rawValue.isEmpty()) {
            return null;
        }

        // Lookup con path > (relaciones)
        if (fs.lookupColumn != null) {
            String currentTable;
            String targetColumn = fs.targetColumn;

            // Recorrer path completo
            if (fs.pathParts.length == 1) {
                // Caso simple: AD_Org_ID[Name]
                currentTable = fs.targetColumn.substring(0, fs.targetColumn.length() - 3);
            } else {
                // Caso complejo: AD_User>C_BPartner_ID[Value]
                currentTable = fs.pathParts[fs.pathParts.length - 2];
            }

            String sqlCount = "SELECT COUNT(*) FROM " + currentTable + " WHERE " + fs.lookupColumn + "=?";
            int count = DB.getSQLValue(get_TrxName(), sqlCount, rawValue);
            if (count <= 0) {
                throw new Exception("Fila " + fileRowNumber + ", columna " + fs.columnIndex +
                        " (" + fs.original + "): No se encontró valor '" + rawValue +
                        "' en " + currentTable + "." + fs.lookupColumn);
            }
            if (count > 1) {
                throw new Exception("Fila " + fileRowNumber + ", columna " + fs.columnIndex +
                        " (" + fs.original + "): Valor ambiguo, se encontraron " + count +
                        " registros en " + currentTable + " para " + fs.lookupColumn + "=" + rawValue);
            }

            // Recuperar valor
            String sql = "SELECT " + targetColumn + " FROM " + currentTable + " WHERE " + fs.lookupColumn + "=?";
            return DB.getSQLValueString(get_TrxName(), sql, rawValue);
        }

        // Caso directo con ID
        if (fs.targetColumn.toUpperCase().endsWith("_ID")) {
            int id = parseIntSafe(rawValue);
            if (id <= 0) {
                throw new Exception("Fila " + fileRowNumber + ", columna " + fs.columnIndex +
                        " (" + fs.original + "): se esperaba un ID numérico, valor='" + rawValue + "'");
            }
            return id;
        }

        return rawValue;
    }

    // FieldSpec representa una columna del header con su metadata (lookup, key, path)
    private static class FieldSpec {
        final String original;        // token tal cual en header
        final String[] pathParts;     // si contiene '>' split por '>' (ej AD_User, C_BPartner_ID)
        final String targetColumn;    // última parte (ej C_BPartner_ID o Name)
        final String lookupColumn;    // si tiene [LookupColumn], ej Name o Value
        final boolean isKey;          // /K
        final int columnIndex;        // posición en header (1-based)

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
}
