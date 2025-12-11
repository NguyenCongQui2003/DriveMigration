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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DriveServiceComplete - PARALLEL PROCESSING VERSION với SMART RATE LIMITING
 *
 * Tính năng:
 * - Xử lý song song nhiều files (3 threads mặc định - AN TOÀN)
 * - Semaphore để giới hạn concurrent requests
 * - Dynamic rate limiting dựa trên API response
 * - Auto-retry với exponential backoff cho lỗi 429
 */
public class DriveServiceComplete {
    private final String serviceAccountEmail;
    private final String privateKey;
    private final List<String> scopes = Arrays.asList(
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file"
    );

    // ===== PARALLEL PROCESSING SETTINGS =====
    private static final int FILE_PROCESSING_THREADS = 3; // Giảm từ 5 xuống 3 để an toàn

    // ===== SMART RATE LIMITING =====
    private final Semaphore requestSemaphore = new Semaphore(8); // Max 8 requests đồng thời
    private final AtomicLong lastWriteTime = new AtomicLong(0);
    private final AtomicInteger currentDelayMs = new AtomicInteger(150); // Dynamic delay, bắt đầu 150ms

    // Min/Max delays
    private static final int MIN_DELAY_MS = 100;
    private static final int MAX_DELAY_MS = 500;

    // ===== ACCESS TOKEN CACHE =====
    // ===== ACCESS TOKEN CACHE - MỖI USER MỘT TOKEN =====
    private final Map<String, String> cachedAccessTokens = new ConcurrentHashMap<>();
    private final Map<String, Long> tokenExpiryTimes = new ConcurrentHashMap<>();
    private final Object tokenLock = new Object();

    public DriveServiceComplete(String serviceAccountEmail, String privateKey) {
        this.serviceAccountEmail = serviceAccountEmail;
        this.privateKey = privateKey;
    }

    /**
     * SMART RATE LIMITING: Tự động điều chỉnh delay dựa trên tình hình API
     */
    private void smartWaitForRateLimit() throws InterruptedException {
        // Acquire semaphore - chờ nếu đã có quá nhiều requests đang chạy
        requestSemaphore.acquire();

        try {
            long now = System.currentTimeMillis();
            long timeSinceLastWrite = now - lastWriteTime.get();
            int currentDelay = currentDelayMs.get();

            if (timeSinceLastWrite < currentDelay) {
                Thread.sleep(currentDelay - timeSinceLastWrite);
            }

            lastWriteTime.set(System.currentTimeMillis());
        } finally {
            // Release semaphore sau khi đã apply delay
            requestSemaphore.release();
        }
    }

    /**
     * Tăng delay khi gặp rate limit
     */
    private void increaseDelay() {
        int current = currentDelayMs.get();
        int newDelay = Math.min(current + 50, MAX_DELAY_MS);
        currentDelayMs.set(newDelay);
        System.out.println("⚠️ Increased delay to " + newDelay + "ms due to rate limiting");
    }

    /**
     * Giảm delay khi API hoạt động tốt
     */
    private void decreaseDelay() {
        int current = currentDelayMs.get();
        int newDelay = Math.max(current - 10, MIN_DELAY_MS);
        if (newDelay < current) {
            currentDelayMs.set(newDelay);
            System.out.println("✓ Decreased delay to " + newDelay + "ms - API stable");
        }
    }

    /**
     * CACHED ACCESS TOKEN
     */
    /**
     * CACHED ACCESS TOKEN - MỖI USER MỘT TOKEN
     */
    private String getAccessToken(String userEmail) throws Exception {
        synchronized (tokenLock) {
            long now = System.currentTimeMillis();

            // ★ KIỂM TRA CACHE CHO USER CỤ THỂ ★
            String cachedToken = cachedAccessTokens.get(userEmail);
            Long expiryTime = tokenExpiryTimes.get(userEmail);

            if (cachedToken != null && expiryTime != null && now < (expiryTime - 300000)) {
                System.out.println("🔑 [CACHE] Using token for: " + userEmail);
                return cachedToken;
            }

            // ★ TẠO TOKEN MỚI CHO USER NÀY ★
            System.out.println("🔑 [NEW] Getting token for: " + userEmail);
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
                try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
                    StringBuilder errorResponse = new StringBuilder();
                    String line;
                    while ((line = errorReader.readLine()) != null) {
                        errorResponse.append(line);
                    }
                    throw new RuntimeException("Failed to get access token for " + userEmail + ": " + responseCode + " - " + errorResponse.toString());
                }
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
                    String newToken = matcher.group(1);
                    long newExpiryTime = now + 3600000;

                    // ★ LƯU TOKEN RIÊNG CHO USER NÀY ★
                    cachedAccessTokens.put(userEmail, newToken);
                    tokenExpiryTimes.put(userEmail, newExpiryTime);

                    System.out.println("✅ [SAVED] Token cached for: " + userEmail);
                    return newToken;
                } else {
                    throw new RuntimeException("Could not extract access token from response: " + jsonResponse);
                }
            }
        }
    }


    /**
     * API REQUEST với RETRY LOGIC và DYNAMIC RATE LIMITING
     */
    private String makeApiRequest(String endpoint, String method, String payload, String userEmail, boolean isWrite) throws Exception {
        String accessToken = getAccessToken(userEmail);

        int retries = 0;
        int maxRetries = 5;

        while (retries <= maxRetries) {
            try {
                // Apply rate limiting cho write operations
                if (isWrite) {
                    smartWaitForRateLimit();
                }

                URL url = new URL(endpoint);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod(method);
                conn.setRequestProperty("Authorization", "Bearer " + accessToken);
                conn.setRequestProperty("Content-Type", "application/json");

                if (payload != null && !payload.isEmpty()) {
                    conn.setDoOutput(true);
                    try (OutputStream os = conn.getOutputStream()) {
                        os.write(payload.getBytes("UTF-8"));
                    }
                }

                int responseCode = conn.getResponseCode();

                // Success!
                if (responseCode >= 200 && responseCode < 300) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"))) {
                        StringBuilder response = new StringBuilder();
                        String line;
                        while ((line = reader.readLine()) != null) {
                            response.append(line);
                        }

                        // API stable - giảm delay xuống
                        if (isWrite && retries == 0) {
                            decreaseDelay();
                        }

                        return response.toString();
                    }
                }

                // Rate limit error - retry với backoff
                if (responseCode == 429 || responseCode == 403) {
                    retries++;

                    // Tăng delay để tránh rate limit tiếp theo
                    increaseDelay();

                    if (retries > maxRetries) {
                        throw new RuntimeException("Max retries exceeded for rate limit");
                    }

                    // Exponential backoff: 2^retries giây
                    long backoffMs = (long) Math.pow(2, retries) * 1000;
                    System.out.println(String.format(
                            "Rate limit hit (429/403). Retry %d/%d after %dms...",
                            retries, maxRetries, backoffMs
                    ));

                    Thread.sleep(backoffMs);
                    continue;
                }

                // Other errors - read error response
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getErrorStream(), "UTF-8"))) {
                    StringBuilder errorResponse = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        errorResponse.append(line);
                    }
                    throw new RuntimeException("API request failed: " + responseCode + " - " + errorResponse.toString());
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Request interrupted", e);
            } catch (RuntimeException e) {
                // Re-throw runtime exceptions
                throw e;
            } catch (Exception e) {
                retries++;
                if (retries > maxRetries) {
                    throw e;
                }
                System.out.println("Request failed, retrying (" + retries + "/" + maxRetries + ")...");
                Thread.sleep(1000 * retries);
            }
        }

        throw new RuntimeException("Max retries exceeded");
    }

    /**
     * GET ALL FILES - Không đổi, vẫn nhanh
     */
    public List<DriveFile> getAllFiles(String userEmail) throws Exception {
        List<DriveFile> files = new ArrayList<>();
        String pageToken = null;

        System.out.println("DEBUG: Starting getAllFiles for user: " + userEmail);

        do {
            String endpoint = "https://www.googleapis.com/drive/v3/files" +
                    "?pageSize=1000" +
                    "&q=trashed=false" +
                    "&fields=nextPageToken,files(id,name,mimeType,owners,permissions(role,emailAddress,type),capabilities)" +
                    (pageToken != null ? "&pageToken=" + pageToken : "");

            String response = makeApiRequest(endpoint, "GET", null, userEmail, false);

            List<DriveFile> pageFiles = parseFilesFromResponse(response);
            files.addAll(pageFiles);

            pageToken = extractNextPageToken(response);

        } while (pageToken != null && files.size() < 10000);

        System.out.println("DEBUG: Total files retrieved for " + userEmail + ": " + files.size());
        return files;
    }

    /**
     * ★★★ PARALLEL PROCESSING - VERSION AN TOÀN ★★★
     */
    public MigrationResult processUserDrive(String userEmail,
                                            Map<String, String> userMapping,
                                            FileProgressCallback callback) throws Exception {
        MigrationResult result = new MigrationResult();
        result.userEmail = userEmail;
        result.startTime = new Date();

        try {
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("🚀 Starting PARALLEL processing for: " + userEmail);
            System.out.println("   Threads: " + FILE_PROCESSING_THREADS);
            System.out.println("   Max concurrent requests: " + requestSemaphore.availablePermits());
            System.out.println("   Initial delay: " + currentDelayMs.get() + "ms");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            // Bước 1: Get all files
            List<DriveFile> files = getAllFiles(userEmail);
            result.totalFiles = files.size();

            if (files.isEmpty()) {
                result.endTime = new Date();
                result.success = true;
                return result;
            }

            // Bước 2: Create thread pool
            ExecutorService fileExecutor = Executors.newFixedThreadPool(
                    FILE_PROCESSING_THREADS,
                    new ThreadFactory() {
                        private final AtomicInteger threadNumber = new AtomicInteger(1);
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread thread = new Thread(r, "FileProcessor-" + threadNumber.getAndIncrement());
                            thread.setDaemon(true);
                            return thread;
                        }
                    }
            );

            // Counters
            AtomicInteger processedCount = new AtomicInteger(0);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            AtomicInteger restrictedCount = new AtomicInteger(0);
            AtomicInteger skippedCount = new AtomicInteger(0);

            // Bước 3: Submit all files
            List<Future<FileProcessingResult>> futures = new ArrayList<>();

            for (DriveFile file : files) {
                Future<FileProcessingResult> future = fileExecutor.submit(() -> {
                    return processFilePermissions(file, userMapping, userEmail);
                });
                futures.add(future);
            }

            System.out.println("✓ Submitted " + files.size() + " files to " +
                    FILE_PROCESSING_THREADS + " worker threads");

            // Bước 4: Collect results
            long lastProgressTime = System.currentTimeMillis();

            for (int i = 0; i < futures.size(); i++) {
                try {
                    FileProcessingResult fileResult = futures.get(i).get(2, TimeUnit.MINUTES); // Timeout 2 phút/file

                    result.fileResults.add(fileResult);

                    int currentCount = processedCount.incrementAndGet();

                    switch (fileResult.status) {
                        case "SUCCESS":
                            successCount.incrementAndGet();
                            break;
                        case "ERROR":
                            errorCount.incrementAndGet();
                            break;
                        case "RESTRICTED":
                            restrictedCount.incrementAndGet();
                            break;
                        case "SKIPPED":
                            skippedCount.incrementAndGet();
                            break;
                    }

                    if (callback != null) {
                        callback.onFileProcessed(userEmail, currentCount, files.size(), fileResult);
                    }

                    // Progress report mỗi 5 giây
                    long now = System.currentTimeMillis();
                    if (now - lastProgressTime > 5000) {
                        double percentage = (double) currentCount / files.size() * 100;
                        System.out.println(String.format(
                                "📊 Progress: %.1f%% (%d/%d) | Success: %d, Error: %d, Restricted: %d, Skipped: %d | Delay: %dms",
                                percentage, currentCount, files.size(),
                                successCount.get(), errorCount.get(),
                                restrictedCount.get(), skippedCount.get(),
                                currentDelayMs.get()
                        ));
                        lastProgressTime = now;
                    }

                } catch (TimeoutException e) {
                    System.err.println("⚠️ Timeout processing file " + (i+1));
                    errorCount.incrementAndGet();
                } catch (Exception e) {
                    System.err.println("⚠️ Error getting result for file " + (i+1) + ": " + e.getMessage());
                    errorCount.incrementAndGet();
                }
            }

            // Bước 5: Shutdown executor
            fileExecutor.shutdown();
            try {
                if (!fileExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
                    fileExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                fileExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Final results
            result.successFiles = successCount.get();
            result.failedFiles = errorCount.get();
            result.restrictedFiles = restrictedCount.get();
            result.skippedFiles = skippedCount.get();
            result.endTime = new Date();
            result.success = true;

            long durationSeconds = (result.endTime.getTime() - result.startTime.getTime()) / 1000;
            double filesPerSecond = files.size() / (double) Math.max(1, durationSeconds);

            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("✅ User " + userEmail + " COMPLETED!");
            System.out.println("   Duration: " + durationSeconds + " seconds");
            System.out.println("   Speed: " + String.format("%.1f", filesPerSecond) + " files/second");
            System.out.println("   Total: " + result.totalFiles + " files");
            System.out.println("   Success: " + result.successFiles);
            System.out.println("   Failed: " + result.failedFiles);
            System.out.println("   Restricted: " + result.restrictedFiles);
            System.out.println("   Skipped: " + result.skippedFiles);
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        } catch (Exception e) {
            result.endTime = new Date();
            result.success = false;
            result.errorMessage = e.getMessage();
            System.err.println("❌ ERROR processing user " + userEmail + ": " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        return result;
    }

    /**
     * Process file permissions - không đổi
     */
    private FileProcessingResult processFilePermissions(DriveFile file, Map<String, String> userMapping, String userEmail) {
        FileProcessingResult result = new FileProcessingResult();
        result.fileName = file.name;
        result.fileId = file.id;
        result.fileType = getFileType(file.mimeType);

        try {
            if (!canShareFile(file)) {
                result.status = "RESTRICTED";
                result.errorMessage = "File sharing is restricted";
                return result;
            }

            if (file.permissions == null || file.permissions.isEmpty()) {
                result.status = "SKIPPED";
                result.errorMessage = "No permissions to process";
                return result;
            }

            boolean hasSuccessfulShare = false;
            boolean hasAnyPermissionToProcess = false;

            for (DrivePermission permission : file.permissions) {
                String oldEmail = permission.emailAddress;

                if ("owner".equals(permission.role)) continue;
                if ("domain".equals(permission.type)) continue;
                if ("anyone".equals(permission.type)) continue;
                if (!"user".equals(permission.type) || oldEmail == null || oldEmail.trim().isEmpty()) continue;

                if (userMapping.containsKey(oldEmail)) {
                    hasAnyPermissionToProcess = true;
                    String newEmail = userMapping.get(oldEmail);

                    result.oldEmail = oldEmail;
                    result.newEmail = newEmail;
                    result.role = permission.role;
                    result.permissionType = getPermissionType(permission.role);

                    try {
                        String permissionPayload = String.format(
                                "{\"type\":\"user\",\"role\":\"%s\",\"emailAddress\":\"%s\"}",
                                permission.role, newEmail
                        );

                        String endpoint = String.format(
                                "https://www.googleapis.com/drive/v3/files/%s/permissions?sendNotificationEmail=false&supportsAllDrives=true",
                                file.id
                        );

                        String response = makeApiRequest(endpoint, "POST", permissionPayload, userEmail, true);

                        hasSuccessfulShare = true;
                        result.status = "SUCCESS";
                        result.permissionsAdded++;

                    } catch (Exception e) {
                        String errorMessage = e.getMessage();

                        if (errorMessage != null && (errorMessage.contains("restricted") ||
                                errorMessage.contains("flagged") ||
                                errorMessage.contains("sharingNotAllowed"))) {
                            result.status = "RESTRICTED";
                            result.errorMessage = errorMessage;
                        } else {
                            result.status = "ERROR";
                            result.errorMessage = errorMessage;
                        }
                    }
                }
            }

            if (!hasAnyPermissionToProcess) {
                result.status = "SKIPPED";
                result.errorMessage = "No permissions to migrate";
            } else if (!hasSuccessfulShare && !"RESTRICTED".equals(result.status)) {
                result.status = "ERROR";
                if (result.errorMessage == null) {
                    result.errorMessage = "Failed to add any permissions";
                }
            }

        } catch (Exception e) {
            result.status = "ERROR";
            result.errorMessage = e.getMessage();
        }

        return result;
    }

    // JWT methods (giữ nguyên)
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
                userEmail
        );
        String payloadBase64 = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes());

        String signatureInput = headerBase64 + "." + payloadBase64;
        String signature = signWithPrivateKey(signatureInput, privateKey);

        return signatureInput + "." + signature;
    }

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

    public boolean testConnection(String userEmail) {
        try {
            String endpoint = "https://www.googleapis.com/drive/v3/about?fields=user";
            String response = makeApiRequest(endpoint, "GET", null, userEmail, false);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean canShareFile(DriveFile file) {
        return true;
    }

    private String getFileType(String mimeType) {
        if ("application/vnd.google-apps.folder".equals(mimeType)) {
            return "Folder";
        }
        return "File";
    }

    private String getPermissionType(String role) {
        switch (role) {
            case "reader":
            case "viewer":
                return "View";
            case "writer":
                return "Edit";
            case "commenter":
                return "Comment";
            case "owner":
                return "Owner";
            default:
                return "Unknown";
        }
    }

    // Parsing methods (giữ nguyên từ code cũ)
    private List<DriveFile> parseFilesFromResponse(String jsonResponse) {
        List<DriveFile> files = new ArrayList<>();
        try {
            int filesIndex = jsonResponse.indexOf("\"files\"");
            if (filesIndex == -1) return files;

            int arrayStart = jsonResponse.indexOf("[", filesIndex);
            if (arrayStart == -1) return files;

            int arrayEnd = findMatchingBracket(jsonResponse, arrayStart);
            if (arrayEnd == -1) return files;

            String filesArrayContent = jsonResponse.substring(arrayStart + 1, arrayEnd);
            if (filesArrayContent.trim().isEmpty()) return files;

            List<String> fileObjects = extractJsonObjects(filesArrayContent);

            for (String fileJson : fileObjects) {
                DriveFile file = parseFileFromJson(fileJson);
                if (file != null) {
                    files.add(file);
                }
            }
        } catch (Exception e) {
            System.err.println("ERROR: Error parsing files response: " + e.getMessage());
        }
        return files;
    }

    private int findMatchingBracket(String json, int startIndex) {
        int count = 0;
        for (int i = startIndex; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '[') count++;
            else if (c == ']') {
                count--;
                if (count == 0) return i;
            }
        }
        return -1;
    }

    private List<String> extractJsonObjects(String arrayContent) {
        List<String> objects = new ArrayList<>();
        int start = 0;
        int braceCount = 0;
        boolean inString = false;
        boolean escaped = false;

        for (int i = 0; i < arrayContent.length(); i++) {
            char c = arrayContent.charAt(i);

            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '"') {
                inString = !inString;
                continue;
            }
            if (inString) continue;

            if (c == '{') {
                if (braceCount == 0) start = i;
                braceCount++;
            } else if (c == '}') {
                braceCount--;
                if (braceCount == 0) {
                    objects.add(arrayContent.substring(start, i + 1));
                }
            }
        }
        return objects;
    }

    private DriveFile parseFileFromJson(String fileJson) {
        try {
            DriveFile file = new DriveFile();
            file.id = extractJsonValue(fileJson, "id");
            if (file.id == null) return null;

            file.name = extractJsonValue(fileJson, "name");
            if (file.name == null) file.name = "Unnamed File";

            file.mimeType = extractJsonValue(fileJson, "mimeType");
            file.permissions = parsePermissionsFromJson(fileJson);

            return file;
        } catch (Exception e) {
            return null;
        }
    }

    private String extractJsonValue(String json, String key) {
        try {
            Pattern pattern = Pattern.compile("\"" + key + "\"\\s*:\\s*\"([^\"\\\\]*(\\\\.[^\"\\\\]*)*)\"");
            Matcher matcher = pattern.matcher(json);
            if (matcher.find()) {
                return matcher.group(1).replace("\\\"", "\"").replace("\\\\", "\\");
            }
        } catch (Exception e) {
        }
        return null;
    }

    private List<DrivePermission> parsePermissionsFromJson(String fileJson) {
        List<DrivePermission> permissions = new ArrayList<>();
        try {
            int permissionsIndex = fileJson.indexOf("\"permissions\"");
            if (permissionsIndex == -1) return permissions;

            int arrayStart = fileJson.indexOf("[", permissionsIndex);
            if (arrayStart == -1) return permissions;

            int arrayEnd = findMatchingBracket(fileJson, arrayStart);
            if (arrayEnd == -1) return permissions;

            String permissionsContent = fileJson.substring(arrayStart + 1, arrayEnd);
            if (permissionsContent.trim().isEmpty()) return permissions;

            List<String> permissionObjects = extractJsonObjects(permissionsContent);

            for (String permissionJson : permissionObjects) {
                DrivePermission permission = new DrivePermission();
                permission.role = extractJsonValue(permissionJson, "role");
                permission.emailAddress = extractJsonValue(permissionJson, "emailAddress");
                permission.type = extractJsonValue(permissionJson, "type");

                if (permission.role != null) {
                    permissions.add(permission);
                }
            }
        } catch (Exception e) {
        }
        return permissions;
    }

    private String extractNextPageToken(String jsonResponse) {
        return extractJsonValue(jsonResponse, "nextPageToken");
    }
}

// Supporting classes
interface FileProgressCallback {
    void onFileProcessed(String userEmail, int currentFile, int totalFiles, FileProcessingResult result);
}

class DriveFile {
    public String id;
    public String name;
    public String mimeType;
    public List<DrivePermission> permissions = new ArrayList<>();
}

class DrivePermission {
    public String role;
    public String emailAddress;
    public String type;
}

class FileProcessingResult {
    public String fileName;
    public String fileId;
    public String fileType;
    public String permissionType;
    public String status;
    public String oldEmail;
    public String newEmail;
    public String role;
    public String errorMessage;
    public int permissionsAdded = 0;
}

class MigrationResult {
    public String userEmail;
    public Date startTime;
    public Date endTime;
    public boolean success;
    public String errorMessage;
    public int totalFiles;
    public int successFiles;
    public int failedFiles;
    public int restrictedFiles;
    public int skippedFiles;
    public List<FileProcessingResult> fileResults = new ArrayList<>();
}

class MigrationStats {
    public int totalFiles;
    public int successFiles;
    public int failedFiles;
    public int restrictedFiles;
    public int skippedFiles;
}