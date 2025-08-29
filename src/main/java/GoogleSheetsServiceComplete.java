import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.Instant;

/**
 * Service class để tương tác với Google Sheets API sử dụng Service Account
 * Tương tự như Google Apps Script nhưng chạy trong Java
 */
public class GoogleSheetsServiceComplete {
    private final String serviceAccountEmail;
    private final String privateKey;
    private final String spreadsheetId;
    private final List<String> scopes = Arrays.asList(
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
    );

    // Column indices
    public static final int EMAIL_COL = 0;
    public static final int START_DATE_COL = 1;
    public static final int END_DATE_COL = 2;
    public static final int STATUS_COL = 3;
    public static final int TOTAL_FILES_COL = 4;
    public static final int SUCCESS_FILES_COL = 5;
    public static final int FAILED_FILES_COL = 6;
    public static final int RESTRICTED_FILES_COL = 7;
    public static final int DETAIL_LINK_COL = 8;

    public GoogleSheetsServiceComplete(String serviceAccountEmail, String privateKey, String spreadsheetId) {
        this.serviceAccountEmail = serviceAccountEmail;
        this.privateKey = privateKey;
        this.spreadsheetId = spreadsheetId;
    }

    /**
     * Tạo JWT token cho Service Account authentication
     */
    private String createJWT(String userEmail) throws Exception {
        long now = Instant.now().getEpochSecond();
        long expiry = now + 3600; // 1 hour

        // Header
        String header = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
        String headerBase64 = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(header.getBytes());

        // Payload
        String payload = String.format(
                "{\"iss\":\"%s\",\"scope\":\"%s\",\"aud\":\"https://oauth2.googleapis.com/token\",\"exp\":%d,\"iat\":%d,\"sub\":\"%s\"}",
                serviceAccountEmail,
                String.join(" ", scopes),
                expiry,
                now,
                userEmail != null ? userEmail : serviceAccountEmail
        );
        String payloadBase64 = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes());

        // Signature
        String signatureInput = headerBase64 + "." + payloadBase64;
        String signature = signWithPrivateKey(signatureInput, privateKey);

        return signatureInput + "." + signature;
    }

    /**
     * Ký JWT với private key
     */
    private String signWithPrivateKey(String data, String privateKeyPem) throws Exception {
        // Clean private key
        String cleanKey = privateKeyPem
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");

        byte[] keyBytes = Base64.getDecoder().decode(cleanKey);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PrivateKey key = keyFactory.generatePrivate(keySpec);

        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(key);
        signature.update(data.getBytes());

        byte[] signatureBytes = signature.sign();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(signatureBytes);
    }

    /**
     * Lấy access token từ JWT
     */
    private String getAccessToken(String userEmail) throws Exception {
        String jwt = createJWT(userEmail);

        URL url = new URL("https://oauth2.googleapis.com/token");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setDoOutput(true);

        String postData = "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=" + jwt;

        try (OutputStream os = conn.getOutputStream()) {
            os.write(postData.getBytes());
        }

        int responseCode = conn.getResponseCode();
        if (responseCode != 200) {
            throw new RuntimeException("Failed to get access token: " + responseCode);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            // Parse JSON response to get access_token
            String jsonResponse = response.toString();
            Pattern pattern = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
            Matcher matcher = pattern.matcher(jsonResponse);

            if (matcher.find()) {
                return matcher.group(1);
            } else {
                throw new RuntimeException("Could not extract access token from response");
            }
        }
    }

    /**
     * Thực hiện HTTP request đến Google Sheets API
     */
    private String makeApiRequest(String endpoint, String method, String payload) throws Exception {
        String accessToken = getAccessToken(serviceAccountEmail);

        URL url = new URL(endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Authorization", "Bearer " + accessToken);
        conn.setRequestProperty("Content-Type", "application/json");

        if (payload != null && !payload.isEmpty()) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.getBytes());
            }
        }

        int responseCode = conn.getResponseCode();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                responseCode >= 200 && responseCode < 300 ? conn.getInputStream() : conn.getErrorStream()))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            if (responseCode >= 200 && responseCode < 300) {
                return response.toString();
            } else {
                throw new RuntimeException("API request failed: " + responseCode + " - " + response.toString());
            }
        }
    }

    /**
     * Đọc danh sách users từ Sheet1
     */
    public List<DriveMigrationToolComplete.UserRecord> getUserList() throws Exception {
        List<DriveMigrationToolComplete.UserRecord> users = new ArrayList<>();

        System.out.println("DEBUG: Starting getUserList()");

        // Initialize headers first
        initializeMainSheet();

        String endpoint = String.format(
                "https://sheets.googleapis.com/v4/spreadsheets/%s/values/Sheet1!A:I",
                spreadsheetId
        );

        System.out.println("DEBUG: Making API request to: " + endpoint);
        String response = makeApiRequest(endpoint, "GET", null);
        System.out.println("DEBUG: API Response received, length: " + response.length());

        // Parse JSON response
        List<List<String>> values = parseValuesFromResponse(response);
        System.out.println("DEBUG: Parsed " + (values != null ? values.size() : 0) + " rows from response");

        if (values == null || values.isEmpty()) {
            System.out.println("DEBUG: No values found, returning empty list");
            return users;
        }

        System.out.println("DEBUG: Processing rows, total rows: " + values.size());

        // Skip header row
        for (int i = 1; i < values.size(); i++) {
            List<String> row = values.get(i);
            System.out.println("DEBUG: Processing row " + i + " with " + row.size() + " columns: " + row);

            if (!row.isEmpty() && !row.get(0).trim().isEmpty()) {
                String email = row.get(0).trim();
                System.out.println("DEBUG: Found email: " + email);

                String status = row.size() > STATUS_COL ?
                        (row.get(STATUS_COL) != null && !row.get(STATUS_COL).trim().isEmpty() ? row.get(STATUS_COL) : "Not Started") :
                        "Not Started";

                DriveMigrationToolComplete.UserRecord user = new DriveMigrationToolComplete.UserRecord(email);
                user.status = status;
                user.rowIndex = i + 1; // 1-based row index

                // Load existing stats if available
                if (row.size() > TOTAL_FILES_COL && !row.get(TOTAL_FILES_COL).isEmpty()) {
                    try {
                        user.totalFiles = Integer.parseInt(row.get(TOTAL_FILES_COL));
                    } catch (NumberFormatException e) {
                        user.totalFiles = 0;
                    }
                }

                if (row.size() > SUCCESS_FILES_COL && !row.get(SUCCESS_FILES_COL).isEmpty()) {
                    try {
                        user.successFiles = Integer.parseInt(row.get(SUCCESS_FILES_COL));
                    } catch (NumberFormatException e) {
                        user.successFiles = 0;
                    }
                }

                if (row.size() > FAILED_FILES_COL && !row.get(FAILED_FILES_COL).isEmpty()) {
                    try {
                        user.failedFiles = Integer.parseInt(row.get(FAILED_FILES_COL));
                    } catch (NumberFormatException e) {
                        user.failedFiles = 0;
                    }
                }

                if (row.size() > RESTRICTED_FILES_COL && !row.get(RESTRICTED_FILES_COL).isEmpty()) {
                    try {
                        user.restrictedFiles = Integer.parseInt(row.get(RESTRICTED_FILES_COL));
                    } catch (NumberFormatException e) {
                        user.restrictedFiles = 0;
                    }
                }

                users.add(user);
                System.out.println("DEBUG: Added user: " + email + " (total users: " + users.size() + ")");
            } else {
                System.out.println("DEBUG: Skipping empty row " + i);
            }
        }

        System.out.println("DEBUG: Returning " + users.size() + " users");
        return users;
    }

    /**
     * Đọc mapping users từ Sheet2
     */
    public Map<String, String> getUserMapping() throws Exception {
        Map<String, String> mapping = new HashMap<>();

        String endpoint = String.format(
                "https://sheets.googleapis.com/v4/spreadsheets/%s/values/Sheet2!A:B",
                spreadsheetId
        );

        String response = makeApiRequest(endpoint, "GET", null);
        List<List<String>> values = parseValuesFromResponse(response);

        if (values == null || values.isEmpty()) {
            return mapping;
        }

        // Skip header row if exists
        int startRow = (values.size() > 0 && !values.get(0).isEmpty() &&
                values.get(0).get(0).toLowerCase().contains("old")) ? 1 : 0;

        for (int i = startRow; i < values.size(); i++) {
            List<String> row = values.get(i);
            if (row.size() >= 2 && !row.get(0).trim().isEmpty() && !row.get(1).trim().isEmpty()) {
                String oldEmail = row.get(0).trim();
                String newEmail = row.get(1).trim();
                mapping.put(oldEmail, newEmail);
            }
        }

        return mapping;
    }

    /**
     * Khởi tạo headers cho Main Sheet
     */
    private void initializeMainSheet() throws Exception {
        List<String> headers = Arrays.asList(
                "Email", "Ngày bắt đầu", "Ngày kết thúc", "Trạng thái",
                "Tổng files", "Files thành công", "Files thất bại",
                "Files bị hạn chế", "Link chi tiết"
        );

        // Check if headers exist
        String endpoint = String.format(
                "https://sheets.googleapis.com/v4/spreadsheets/%s/values/Sheet1!A1:I1",
                spreadsheetId
        );

        try {
            String response = makeApiRequest(endpoint, "GET", null);
            List<List<String>> values = parseValuesFromResponse(response);

            boolean needsHeaders = true;
            if (values != null && !values.isEmpty() && values.get(0).size() >= headers.size()) {
                List<String> firstRow = values.get(0);
                needsHeaders = false;
                for (int i = 0; i < headers.size(); i++) {
                    if (i >= firstRow.size() || !headers.get(i).equals(firstRow.get(i))) {
                        needsHeaders = true;
                        break;
                    }
                }
            }

            if (needsHeaders) {
                // Update headers
                String updateEndpoint = String.format(
                        "https://sheets.googleapis.com/v4/spreadsheets/%s/values/Sheet1!A1:I1?valueInputOption=RAW",
                        spreadsheetId
                );

                String payload = "{\"values\":[" +
                        "[\"" + String.join("\",\"", headers) + "\"]" +
                        "]}";

                makeApiRequest(updateEndpoint, "PUT", payload);
            }
        } catch (Exception e) {
            // If sheet doesn't exist or error, create headers anyway
            String updateEndpoint = String.format(
                    "https://sheets.googleapis.com/v4/spreadsheets/%s/values/Sheet1!A1:I1?valueInputOption=RAW",
                    spreadsheetId
            );

            String payload = "{\"values\":[" +
                    "[\"" + String.join("\",\"", headers) + "\"]" +
                    "]}";

            makeApiRequest(updateEndpoint, "PUT", payload);
        }
    }

    /**
     * Cập nhật trạng thái user
     */
    public void updateUserStatus(String userEmail, int rowIndex, String status, MigrationStats stats) throws Exception {
        List<String> updates = new ArrayList<>();

        if ("In Progress".equals(status)) {
            updates.add(new Date().toString()); // Start date
            updates.add(""); // End date (empty)
            updates.add(status); // Status
        } else if ("Completed".equals(status) || "Failed".equals(status)) {
            // Get current start date
            String currentStartDate = "";
            try {
                String endpoint = String.format(
                        "https://sheets.googleapis.com/v4/spreadsheets/%s/values/Sheet1!B%d:B%d",
                        spreadsheetId, rowIndex, rowIndex
                );
                String response = makeApiRequest(endpoint, "GET", null);
                List<List<String>> values = parseValuesFromResponse(response);

                if (values != null && !values.isEmpty() && !values.get(0).isEmpty()) {
                    currentStartDate = values.get(0).get(0);
                }
            } catch (Exception e) {
                // Ignore error, use current date
            }

            if (currentStartDate.isEmpty()) {
                currentStartDate = new Date().toString();
            }

            updates.add(currentStartDate); // Keep existing start date
            updates.add(new Date().toString()); // End date
            updates.add(status); // Status

            if (stats != null) {
                updates.add(String.valueOf(stats.totalFiles));
                updates.add(String.valueOf(stats.successFiles));
                updates.add(String.valueOf(stats.failedFiles));
                updates.add(String.valueOf(stats.restrictedFiles));
                updates.add(createDetailSheetLink(userEmail));
            }
        }

        if (!updates.isEmpty()) {
            String range = String.format("Sheet1!B%d:%s%d", rowIndex,
                    (char)('A' + updates.size()), rowIndex);

            String endpoint = String.format(
                    "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s?valueInputOption=RAW",
                    spreadsheetId, range
            );

            String payload = "{\"values\":[" +
                    "[\"" + String.join("\",\"", updates) + "\"]" +
                    "]}";

            makeApiRequest(endpoint, "PUT", payload);
        }
    }

    /**
     * Tạo hoặc cập nhật detail sheet cho user
     */
    public void createOrUpdateUserDetailSheet(String userEmail, List<FileProcessingResult> results) throws Exception {
        String sheetName = "Detail_" + userEmail.replace("@", "_at_").replace(".", "_");

        // Check if sheet exists (simplified - assume it doesn't exist for now)
        // In a full implementation, you would check existing sheets first

        try {
            // Create new sheet
            String createEndpoint = String.format(
                    "https://sheets.googleapis.com/v4/spreadsheets/%s:batchUpdate",
                    spreadsheetId
            );

            String createPayload = String.format(
                    "{\"requests\":[{\"addSheet\":{\"properties\":{\"title\":\"%s\"}}}]}",
                    sheetName
            );

            try {
                makeApiRequest(createEndpoint, "POST", createPayload);
            } catch (Exception e) {
                // Sheet might already exist, continue
            }

            // Add headers
            List<String> headers = Arrays.asList(
                    "Timestamp", "File Name", "File ID", "Type", "Permission Type",
                    "Status", "Old Email", "New Email", "Role", "Error Message"
            );

            String headerEndpoint = String.format(
                    "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s!A1:J1?valueInputOption=RAW",
                    spreadsheetId, sheetName
            );

            String headerPayload = "{\"values\":[" +
                    "[\"" + String.join("\",\"", headers) + "\"]" +
                    "]}";

            makeApiRequest(headerEndpoint, "PUT", headerPayload);

            // Add results data
            if (results != null && !results.isEmpty()) {
                List<List<String>> data = new ArrayList<>();

                for (FileProcessingResult result : results) {
                    List<String> row = Arrays.asList(
                            new Date().toString(),
                            result.fileName != null ? result.fileName : "",
                            result.fileId != null ? result.fileId : "",
                            result.fileType != null ? result.fileType : "",
                            result.permissionType != null ? result.permissionType : "",
                            result.status != null ? result.status : "",
                            result.oldEmail != null ? result.oldEmail : "",
                            result.newEmail != null ? result.newEmail : "",
                            result.role != null ? result.role : "",
                            result.errorMessage != null ? result.errorMessage : ""
                    );
                    data.add(row);
                }

                // Convert data to JSON
                StringBuilder dataJson = new StringBuilder();
                dataJson.append("{\"values\":[");
                for (int i = 0; i < data.size(); i++) {
                    if (i > 0) dataJson.append(",");
                    dataJson.append("[\"").append(String.join("\",\"", data.get(i))).append("\"]");
                }
                dataJson.append("]}");

                String dataEndpoint = String.format(
                        "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s!A2:J%d?valueInputOption=RAW",
                        spreadsheetId, sheetName, data.size() + 1
                );

                makeApiRequest(dataEndpoint, "PUT", dataJson.toString());
            }

        } catch (Exception e) {
            System.err.println("Error creating detail sheet: " + e.getMessage());
            // Continue without failing the whole process
        }
    }

    /**
     * Tạo link đến detail sheet
     */
    private String createDetailSheetLink(String userEmail) {
        String sheetName = "Detail_" + userEmail.replace("@", "_at_").replace(".", "_");
        return String.format("https://docs.google.com/spreadsheets/d/%s/edit#gid=0&search=%s",
                spreadsheetId, sheetName);
    }

    /**
     * Ghi log vào Migration_Logs sheet
     */
    public void writeLog(String level, String message, String details) throws Exception {
        String logSheetName = "Migration_Logs";

        try {
            // Try to create log sheet (will fail if exists, which is fine)
            String createEndpoint = String.format(
                    "https://sheets.googleapis.com/v4/spreadsheets/%s:batchUpdate",
                    spreadsheetId
            );

            String createPayload = String.format(
                    "{\"requests\":[{\"addSheet\":{\"properties\":{\"title\":\"%s\"}}}]}",
                    logSheetName
            );

            try {
                makeApiRequest(createEndpoint, "POST", createPayload);

                // Add headers for new sheet
                List<String> headers = Arrays.asList("Timestamp", "Level", "Message", "Details");
                String headerEndpoint = String.format(
                        "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s!A1:D1?valueInputOption=RAW",
                        spreadsheetId, logSheetName
                );

                String headerPayload = "{\"values\":[" +
                        "[\"" + String.join("\",\"", headers) + "\"]" +
                        "]}";

                makeApiRequest(headerEndpoint, "PUT", headerPayload);
            } catch (Exception e) {
                // Sheet already exists, continue
            }

            // Append log entry
            String appendEndpoint = String.format(
                    "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s!A:D:append?valueInputOption=RAW",
                    spreadsheetId, logSheetName
            );

            String logPayload = String.format(
                    "{\"values\":[[\"%s\",\"%s\",\"%s\",\"%s\"]]}",
                    new Date().toString(), level, message, details != null ? details : ""
            );

            makeApiRequest(appendEndpoint, "POST", logPayload);

        } catch (Exception e) {
            // Ignore logging errors to prevent infinite loops
            System.err.println("Error writing log: " + e.getMessage());
        }
    }

    /**
     * Parse values từ JSON response của Google Sheets API
     */
    private List<List<String>> parseValuesFromResponse(String jsonResponse) {
        List<List<String>> values = new ArrayList<>();

        try {
            System.out.println("DEBUG: Raw JSON Response: " + jsonResponse);

            // Check if response contains values
            if (!jsonResponse.contains("\"values\"")) {
                System.out.println("DEBUG: No 'values' field found in response");
                return values;
            }

            // Find the values array using manual parsing
            int valuesStart = jsonResponse.indexOf("\"values\":");
            if (valuesStart == -1) {
                System.out.println("DEBUG: Could not find 'values:' in response");
                return values;
            }

            // Find the opening bracket of the values array
            int arrayStart = jsonResponse.indexOf("[", valuesStart);
            if (arrayStart == -1) {
                System.out.println("DEBUG: Could not find opening bracket for values array");
                return values;
            }

            // Find the matching closing bracket
            int bracketCount = 0;
            int arrayEnd = -1;
            for (int i = arrayStart; i < jsonResponse.length(); i++) {
                char c = jsonResponse.charAt(i);
                if (c == '[') {
                    bracketCount++;
                } else if (c == ']') {
                    bracketCount--;
                    if (bracketCount == 0) {
                        arrayEnd = i;
                        break;
                    }
                }
            }

            if (arrayEnd == -1) {
                System.out.println("DEBUG: Could not find closing bracket for values array");
                return values;
            }

            String valuesContent = jsonResponse.substring(arrayStart + 1, arrayEnd);
            System.out.println("DEBUG: Values content: " + valuesContent);

            // Parse each row manually
            List<String> currentRow = new ArrayList<>();
            boolean inQuotes = false;
            boolean inArray = false;
            StringBuilder currentCell = new StringBuilder();

            for (int i = 0; i < valuesContent.length(); i++) {
                char c = valuesContent.charAt(i);

                if (c == '"' && (i == 0 || valuesContent.charAt(i-1) != '\\')) {
                    inQuotes = !inQuotes;
                } else if (!inQuotes) {
                    if (c == '[') {
                        inArray = true;
                        currentRow = new ArrayList<>();
                    } else if (c == ']') {
                        if (currentCell.length() > 0) {
                            currentRow.add(currentCell.toString());
                            currentCell = new StringBuilder();
                        }
                        if (!currentRow.isEmpty()) {
                            values.add(new ArrayList<>(currentRow));
                            System.out.println("DEBUG: Added row: " + currentRow);
                        }
                        inArray = false;
                    } else if (c == ',' && inArray) {
                        currentRow.add(currentCell.toString());
                        currentCell = new StringBuilder();
                    } else if (inArray && c != ' ' && c != '\n' && c != '\r' && c != '\t') {
                        currentCell.append(c);
                    }
                } else if (inQuotes && inArray) {
                    currentCell.append(c);
                }
            }

            System.out.println("DEBUG: Total rows parsed: " + values.size());

        } catch (Exception e) {
            System.err.println("Error parsing JSON response: " + e.getMessage());
            e.printStackTrace();
        }

        return values;
    }
}