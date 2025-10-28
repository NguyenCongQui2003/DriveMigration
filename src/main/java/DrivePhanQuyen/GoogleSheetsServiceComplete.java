package DrivePhanQuyen;

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
import java.util.concurrent.*;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

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
    private final Map<String, Queue<FileProcessingResult>> updateQueues = new ConcurrentHashMap<>();
    private final Map<String, String> sheetNameCache = new ConcurrentHashMap<>();
    private final Map<String, Integer> sheetIdCache = new ConcurrentHashMap<>();
    private final Object batchLock = new Object();

    // RATE LIMITING - QUAN TRỌNG
    private long lastApiCall = 0;
    private final long API_CALL_INTERVAL = 1200; // 1.2 giây giữa các calls
    private final int MAX_RETRIES = 5;

    // BATCH SETTINGS - TĂNG BATCH SIZE
    private static final int FLUSH_BATCH_SIZE = 100; // Tăng từ 20 lên 100
    private static final int AUTO_FLUSH_THRESHOLD = 200; // Tăng từ 10 lên 200


    public GoogleSheetsServiceComplete(String serviceAccountEmail, String privateKey, String spreadsheetId) {
        this.serviceAccountEmail = serviceAccountEmail;
        this.privateKey = privateKey;
        this.spreadsheetId = spreadsheetId;
    }

    /**
     * IMPROVED: Rate limiting với exponential backoff
     */
    private void waitForRateLimit() {
        long now = System.currentTimeMillis();
        long timeSinceLastCall = now - lastApiCall;
        if (timeSinceLastCall < API_CALL_INTERVAL) {
            try {
                long sleepTime = API_CALL_INTERVAL - timeSinceLastCall;
                System.out.println("DEBUG: Rate limiting - sleeping " + sleepTime + "ms");
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        lastApiCall = System.currentTimeMillis();
    }

    /**
     * IMPROVED: Retry logic với exponential backoff
     */
    private String makeApiRequestWithRetry(String endpoint, String method, String payload) throws Exception {
        int retries = 0;
        Exception lastException = null;

        while (retries < MAX_RETRIES) {
            try {
                waitForRateLimit();
                return makeApiRequest(endpoint, method, payload);
            } catch (Exception e) {
                lastException = e;
                String errorMsg = e.getMessage();

                // Check if it's a rate limit error
                if (errorMsg != null && (errorMsg.contains("429") || errorMsg.contains("RATE_LIMIT_EXCEEDED"))) {
                    retries++;

                    // Exponential backoff: 2^retries seconds
                    long backoffTime = (long) Math.pow(2, retries) * 1000;
                    System.out.println("RATE LIMIT: Retry " + retries + "/" + MAX_RETRIES +
                            " - waiting " + backoffTime + "ms");

                    try {
                        Thread.sleep(backoffTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new Exception("Interrupted during backoff", ie);
                    }
                } else {
                    // Not a rate limit error, throw immediately
                    throw e;
                }
            }
        }

        throw new Exception("Max retries exceeded for API request", lastException);
    }

    /**
     * Tạo detail sheet ngay khi bắt đầu xử lý user
     */
    // Thêm map để lưu Sheet ID
    //private final Map<String, Integer> sheetIdCache = new ConcurrentHashMap<>();

    /**
     * Tạo detail sheet ngay khi bắt đầu xử lý user
     */
    public void createUserDetailSheetEarly(String userEmail) throws Exception {
        String sheetName = "Detail_" + userEmail.replace("@", "_at_").replace(".", "_");
        sheetNameCache.put(userEmail, sheetName);

        System.out.println("DEBUG: Creating detail sheet for user: " + userEmail);

        try {
            String createEndpoint = String.format(
                    "https://sheets.googleapis.com/v4/spreadsheets/%s:batchUpdate",
                    spreadsheetId
            );

            String createPayload = String.format(
                    "{\"requests\":[{\"addSheet\":{\"properties\":{\"title\":\"%s\"}}}]}",
                    sheetName
            );

            try {
                String response = makeApiRequestWithRetry(createEndpoint, "POST", createPayload);
                System.out.println("DEBUG: Successfully created sheet: " + sheetName);

                Integer sheetId = extractSheetIdFromResponse(response);
                if (sheetId != null) {
                    sheetIdCache.put(userEmail, sheetId);
                    System.out.println("DEBUG: Cached sheet ID: " + sheetId + " for " + userEmail);
                }

            } catch (Exception e) {
                if (!e.getMessage().contains("already exists")) {
                    throw e;
                }
                System.out.println("DEBUG: Sheet already exists: " + sheetName);

                Integer existingSheetId = getExistingSheetId(sheetName);
                if (existingSheetId != null) {
                    sheetIdCache.put(userEmail, existingSheetId);
                    System.out.println("DEBUG: Found existing sheet ID: " + existingSheetId);
                }
            }

            // Thêm headers
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

            makeApiRequestWithRetry(headerEndpoint, "PUT", headerPayload);

            // Khởi tạo queue
            updateQueues.put(userEmail, new LinkedList<>());

            System.out.println("DEBUG: Detail sheet ready for user: " + userEmail);

        } catch (Exception e) {
            System.err.println("ERROR: Failed to create detail sheet for " + userEmail + ": " + e.getMessage());
            throw e;
        }
    }

    /**
     * IMPROVED: Queue file results với auto-flush ít thường xuyên hơn
     */
    public void appendFileResultToDetailSheet(String userEmail, FileProcessingResult result) throws Exception {
        synchronized (batchLock) {
            Queue<FileProcessingResult> queue = updateQueues.get(userEmail);
            if (queue != null) {
                queue.offer(result);

                // THAY ĐỔI: Chỉ flush khi queue đủ LỚN (200 thay vì 10)
                if (queue.size() >= AUTO_FLUSH_THRESHOLD) {
                    System.out.println("AUTO-FLUSH: Queue size reached " + AUTO_FLUSH_THRESHOLD);
                    flushUserUpdates(userEmail);
                }
            }
        }
    }

    /**
     * Flush tất cả pending updates cho một user
     */
    /**
     * Flush tất cả pending updates cho một user - SỬ DỤNG APPEND
     */
    /**
     * IMPROVED: Flush với batch lớn hơn (100 files/request)
     */
    private void flushUserUpdates(String userEmail) throws Exception {
        Queue<FileProcessingResult> queue = updateQueues.get(userEmail);
        if (queue == null || queue.isEmpty()) {
            return;
        }

        List<FileProcessingResult> batch = new ArrayList<>();

        // THAY ĐỔI: Lấy 100 files mỗi lần thay vì 20
        while (!queue.isEmpty() && batch.size() < FLUSH_BATCH_SIZE) {
            batch.add(queue.poll());
        }

        if (batch.isEmpty()) {
            return;
        }

        System.out.println("FLUSH: Flushing " + batch.size() + " file results for " + userEmail);

        String sheetName = sheetNameCache.get(userEmail);
        if (sheetName == null) {
            System.err.println("ERROR: No sheet name found for user: " + userEmail);
            return;
        }

        try {
            // Prepare batch data
            List<List<String>> data = new ArrayList<>();
            for (FileProcessingResult result : batch) {
                List<String> row = Arrays.asList(
                        getCurrentVietnameseDateTime(),
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

            // APPEND với retry
            String appendEndpoint = String.format(
                    "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s!A:J:append?valueInputOption=RAW&insertDataOption=INSERT_ROWS",
                    spreadsheetId, sheetName
            );

            StringBuilder dataJson = new StringBuilder();
            dataJson.append("{\"values\":[");
            for (int i = 0; i < data.size(); i++) {
                if (i > 0) dataJson.append(",");
                dataJson.append("[\"").append(String.join("\",\"", data.get(i))).append("\"]");
            }
            dataJson.append("]}");

            makeApiRequestWithRetry(appendEndpoint, "POST", dataJson.toString());
            System.out.println("FLUSH SUCCESS: Appended " + batch.size() + " results to " + sheetName);

        } catch (Exception e) {
            System.err.println("FLUSH ERROR: Failed to flush updates for " + userEmail + ": " + e.getMessage());

            // THAY ĐỔI: Re-queue với giới hạn để tránh memory leak
            if (queue.size() < 1000) { // Giới hạn 1000 items trong queue
                for (FileProcessingResult result : batch) {
                    queue.offer(result);
                }
                System.out.println("Re-queued " + batch.size() + " results for retry");
            } else {
                System.err.println("WARNING: Queue too large, dropping " + batch.size() + " results");
            }
        }
    }

    /**
     * Format ngày giờ sang tiếng Việt
     */
    private String formatVietnameseDateTime(Date date) {
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("Asia/Ho_Chi_Minh"));

        String formatted = sdf.format(date);

        // Thêm chữ "Ngày" ở đầu nếu muốn
        return formatted;

        // HOẶC format đầy đủ hơn:
        // return "Ngày " + formatted;
    }

    /**
     * Lấy ngày giờ hiện tại theo định dạng Việt Nam
     */
    private String getCurrentVietnameseDateTime() {
        return formatVietnameseDateTime(new Date());
    }


    /**
     * IMPROVED: Flush all với progress tracking
     */
    public void flushAllPendingUpdates() {
        synchronized (batchLock) {
            System.out.println("=== FINAL FLUSH: Starting ===");

            int totalUsers = updateQueues.size();
            int processedUsers = 0;

            for (String userEmail : updateQueues.keySet()) {
                Queue<FileProcessingResult> queue = updateQueues.get(userEmail);
                int queueSize = queue != null ? queue.size() : 0;

                if (queueSize > 0) {
                    System.out.println("FINAL FLUSH: User " + userEmail + " has " + queueSize + " pending updates");

                    try {
                        // Flush nhiều lần nếu cần
                        while (queue != null && !queue.isEmpty()) {
                            flushUserUpdates(userEmail);

                            // Đợi thêm giữa các batch để tránh rate limit
                            if (!queue.isEmpty()) {
                                Thread.sleep(2000);
                            }
                        }
                        System.out.println("FINAL FLUSH: Completed for " + userEmail);
                    } catch (Exception e) {
                        System.err.println("FINAL FLUSH ERROR: Failed for " + userEmail + ": " + e.getMessage());
                    }
                }

                processedUsers++;
                System.out.println("FINAL FLUSH: Progress " + processedUsers + "/" + totalUsers);
            }

            System.out.println("=== FINAL FLUSH: Completed ===");
        }
    }

    /**
     * Tạo JWT token cho Service Account authentication
     */
    /**
     * JWT và authentication methods (giữ nguyên)
     */
    private String createJWT(String userEmail) throws Exception {
        long now = Instant.now().getEpochSecond();
        long expiry = now + 3600;

        String header = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
        String headerBase64 = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(header.getBytes());

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

        String signatureInput = headerBase64 + "." + payloadBase64;
        String signature = signWithPrivateKey(signatureInput, privateKey);

        return signatureInput + "." + signature;
    }

    /**
     * Ký JWT với private key
     */
    private String signWithPrivateKey(String data, String privateKeyPem) throws Exception {
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
    /**
     * Cập nhật trạng thái user
     */
    public void updateUserStatus(String userEmail, int rowIndex, String status, MigrationStats stats) throws Exception {
        System.out.println("DEBUG updateUserStatus: email=" + userEmail + ", row=" + rowIndex + ", status=" + status);

        List<String> updates = new ArrayList<>();

        if ("In Progress".equals(status)) {
            updates.add(getCurrentVietnameseDateTime()); // Start date - THAY ĐỔI
            updates.add(""); // End date (empty)
            updates.add(status); // Status
            updates.add(""); // Total files - will be updated later
            updates.add(""); // Success
            updates.add(""); // Failed
            updates.add(""); // Restricted
            updates.add(createDetailSheetLink(userEmail)); // Link chi tiết

            System.out.println("DEBUG: Setting In Progress with " + updates.size() + " columns");

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
                System.out.println("DEBUG: Could not get start date: " + e.getMessage());
            }

            if (currentStartDate.isEmpty()) {
                currentStartDate = getCurrentVietnameseDateTime(); // THAY ĐỔI
            }

            updates.add(currentStartDate); // Keep existing start date
            updates.add(getCurrentVietnameseDateTime()); // End date - THAY ĐỔI
            updates.add(status); // Status

            if (stats != null) {
                updates.add(String.valueOf(stats.totalFiles));
                updates.add(String.valueOf(stats.successFiles));
                updates.add(String.valueOf(stats.failedFiles));
                updates.add(String.valueOf(stats.restrictedFiles));
                updates.add(createDetailSheetLink(userEmail));
            } else {
                updates.add("0"); // Total files
                updates.add("0"); // Success
                updates.add("0"); // Failed
                updates.add("0"); // Restricted
                updates.add(createDetailSheetLink(userEmail));
            }

            System.out.println("DEBUG: Setting " + status + " with " + updates.size() + " columns");
        } else if ("Not Started".equals(status)) {
            // RESET
            updates.add(""); // Start date - empty
            updates.add(""); // End date - empty
            updates.add(status); // Status
            updates.add("0"); // Total files
            updates.add("0"); // Success
            updates.add("0"); // Failed
            updates.add("0"); // Restricted
            updates.add(""); // Detail link - empty khi reset

            System.out.println("DEBUG: Resetting to Not Started with " + updates.size() + " columns");
        }

        if (!updates.isEmpty()) {
            // Calculate range based on number of updates
            char endColumn = (char)('A' + updates.size());
            String range = String.format("Sheet1!B%d:%c%d", rowIndex, endColumn, rowIndex);

            String endpoint = String.format(
                    "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s?valueInputOption=RAW",
                    spreadsheetId, range
            );

            // Build JSON payload
            StringBuilder jsonBuilder = new StringBuilder();
            jsonBuilder.append("{\"values\":[[");
            for (int i = 0; i < updates.size(); i++) {
                if (i > 0) jsonBuilder.append(",");
                jsonBuilder.append("\"").append(updates.get(i).replace("\"", "\\\"")).append("\"");
            }
            jsonBuilder.append("]]}");

            String payload = jsonBuilder.toString();

            System.out.println("DEBUG: Updating range: " + range);
            System.out.println("DEBUG: Payload: " + payload);

            String response = makeApiRequest(endpoint, "PUT", payload);

            System.out.println("DEBUG: Update response: " + response);
            System.out.println("✓ Updated user " + userEmail + " to status: " + status);
        } else {
            System.out.println("DEBUG: No updates to perform for status: " + status);
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
                            getCurrentVietnameseDateTime(), // THAY ĐỔI
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
    /**
     * Tạo link đến detail sheet với Sheet ID chính xác
     */
    public String createDetailSheetLink(String userEmail) {
        Integer sheetId = sheetIdCache.get(userEmail);

        if (sheetId != null) {
            // Link với Sheet ID chính xác
            return String.format("https://docs.google.com/spreadsheets/d/%s/edit#gid=%d",
                    spreadsheetId, sheetId);
        } else {
            // Fallback: tìm sheet ID realtime (chậm hơn)
            String sheetName = "Detail_" + userEmail.replace("@", "_at_").replace(".", "_");
            Integer foundSheetId = getExistingSheetId(sheetName);

            if (foundSheetId != null) {
                sheetIdCache.put(userEmail, foundSheetId); // Cache lại
                return String.format("https://docs.google.com/spreadsheets/d/%s/edit#gid=%d",
                        spreadsheetId, foundSheetId);
            }

            // Last resort: dùng search (không đáng tin cậy)
            System.err.println("WARNING: Could not find sheet ID for " + userEmail + ", using search fallback");
            return String.format("https://docs.google.com/spreadsheets/d/%s/edit#search=%s",
                    spreadsheetId, sheetName);
        }
    }

    /**
     * Extract sheet ID từ response khi tạo sheet mới
     */
    private Integer extractSheetIdFromResponse(String jsonResponse) {
        try {
            // Response format: {"replies":[{"addSheet":{"properties":{"sheetId":123,"title":"..."}}}]}
            Pattern pattern = Pattern.compile("\"sheetId\"\\s*:\\s*(\\d+)");
            Matcher matcher = pattern.matcher(jsonResponse);
            if (matcher.find()) {
                return Integer.parseInt(matcher.group(1));
            }
        } catch (Exception e) {
            System.err.println("ERROR: Could not extract sheet ID: " + e.getMessage());
        }
        return null;
    }

    /**
     * Lấy Sheet ID của sheet đã tồn tại
     */
    private Integer getExistingSheetId(String sheetName) {
        try {
            String endpoint = String.format(
                    "https://sheets.googleapis.com/v4/spreadsheets/%s?fields=sheets(properties(sheetId,title))",
                    spreadsheetId
            );

            String response = makeApiRequest(endpoint, "GET", null);

            // Parse response để tìm sheetId tương ứng với sheetName
            String searchPattern = "\"title\"\\s*:\\s*\"" + Pattern.quote(sheetName) + "\"";
            Pattern titlePattern = Pattern.compile(searchPattern);
            Matcher titleMatcher = titlePattern.matcher(response);

            if (titleMatcher.find()) {
                int titlePos = titleMatcher.start();

                // Tìm sheetId gần nhất trước title này
                String beforeTitle = response.substring(0, titlePos);
                Pattern idPattern = Pattern.compile("\"sheetId\"\\s*:\\s*(\\d+)");
                Matcher idMatcher = idPattern.matcher(beforeTitle);

                Integer lastSheetId = null;
                while (idMatcher.find()) {
                    lastSheetId = Integer.parseInt(idMatcher.group(1));
                }

                return lastSheetId;
            }

        } catch (Exception e) {
            System.err.println("ERROR: Could not get existing sheet ID: " + e.getMessage());
        }
        return null;
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
                    getCurrentVietnameseDateTime(), level, message, details != null ? details : ""
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